import * as Y from 'yjs'
import * as uws from 'uws'
import * as promise from 'lib0/promise'
import * as api from './api.js'
import * as array from 'lib0/array'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'
import * as protocol from './protocol.js'
import * as logging from 'lib0/logging'
import { createSubscriber } from './subscriber.js'

const log = logging.createModuleLogger('@y/redis/ws')

/**
 * Deadline for `client.getDoc()` inside the WS open handler. Generous for a
 * normal Firestore-backed read (~50 ms observed) but well under Cloud Run's
 * WS idle timeout, so a hang lands here as a rejection — surfaced by the
 * catch in `open` — instead of as a silent stalled connection.
 */
const OPEN_GET_DOC_TIMEOUT_MS = 10_000

/**
 * Races `p` against a timer that rejects with `<label> timed out after Nms`.
 * The timer is always cleared via `.finally`, so a winning `p` does not leave
 * a setTimeout dangling on the event loop.
 *
 * @template T
 * @param {Promise<T>} p
 * @param {number} ms
 * @param {string} label
 * @returns {Promise<T>}
 */
const withTimeout = (p, ms, label) => {
  /** @type {ReturnType<typeof setTimeout> | undefined} */
  let timer
  const deadline = new Promise((_resolve, reject) => {
    timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms)
  })
  return Promise.race([p, deadline]).finally(() => clearTimeout(timer))
}

/**
 * how to sync
 *   receive sync-step 1
 *   // @todo y-websocket should only accept updates after receiving sync-step 2
 *   redisId = ws.sub(conn)
 *   {doc,redisDocLastId} = api.getdoc()
 *   compute sync-step 2
 *   if (redisId > redisDocLastId) {
 *     subscriber.ensureId(redisDocLastId)
 *   }
 */

class YWebsocketServer {
  /**
   * @param {uws.TemplatedApp} app
   * @param {api.Api} client
   * @param {import('./subscriber.js').Subscriber} subscriber
   */
  constructor (app, client, subscriber) {
    this.app = app
    this.subscriber = subscriber
    this.client = client
  }

  async destroy () {
    this.subscriber.destroy()
    await this.client.destroy()
  }
}

let _idCnt = 0

class User {
  /**
   * @param {string} room
   * @param {boolean} hasWriteAccess
   * @param {string} userid identifies the user globally.
   */
  constructor (room, hasWriteAccess, userid) {
    this.room = room
    this.hasWriteAccess = hasWriteAccess
    /**
     * @type {string}
     */
    this.initialRedisSubId = '0'
    this.subs = new Set()
    /**
     * This is just an identifier to keep track of the user for logging purposes.
     */
    this.id = _idCnt++
    /**
     * Identifies the User globally.
     * Note that several clients can have the same userid (e.g. if a user opened several browser
     * windows)
     */
    this.userid = userid
    /**
     * @type {number|null}
     */
    this.awarenessId = null
    this.awarenessLastClock = 0
    this.isClosed = false
  }
}

/**
 * @param {uws.TemplatedApp} app
 * @param {uws.RecognizedString} pattern
 * @param {import('./storage.js').AbstractStorage} store
 * @param {function(uws.HttpRequest): Promise<{ hasWriteAccess: boolean, room: string, userid: string }>} checkAuth
 * @param {Object} conf
 * @param {string} [conf.redisPrefix]
 * @param {(room:string,docname:string,client:api.Api)=>void} [conf.initDocCallback] - this is called when a doc is
 * accessed, but it doesn't exist. You could populate the doc here. However, this function could be
 * called several times, until some content exists. So you need to handle concurrent calls.
 */
export const registerYWebsocketServer = async (app, pattern, store, checkAuth, { redisPrefix = 'y', initDocCallback = () => {} } = {}) => {
  const [client, subscriber] = await promise.all([
    api.createApiClient(store, redisPrefix),
    createSubscriber(store, redisPrefix)
  ])
  /**
   * @param {string} stream
   * @param {Array<Uint8Array>} messages
   */
  const redisMessageSubscriber = (stream, messages) => {
    if (app.numSubscribers(stream) === 0) {
      subscriber.unsubscribe(stream, redisMessageSubscriber)
    }
    const message = messages.length === 1
      ? messages[0]
      : encoding.encode(encoder => messages.forEach(message => {
        encoding.writeUint8Array(encoder, message)
      }))
    app.publish(stream, message, true, false)
  }
  app.ws(pattern, /** @type {uws.WebSocketBehavior<User>} */ ({
    compression: uws.SHARED_COMPRESSOR,
    maxPayloadLength: 100 * 1024 * 1024,
    idleTimeout: 60,
    sendPingsAutomatically: true,
    upgrade: async (res, req, context) => {
      const url = req.getUrl()
      const headerWsKey = req.getHeader('sec-websocket-key')
      const headerWsProtocol = req.getHeader('sec-websocket-protocol')
      const headerWsExtensions = req.getHeader('sec-websocket-extensions')
      let aborted = false
      res.onAborted(() => {
        console.log('Upgrading client aborted', { url })
        aborted = true
      })
      try {
        const { hasWriteAccess, room, userid } = await checkAuth(req)
        if (aborted) return
        res.cork(() => {
          res.upgrade(
            new User(room, hasWriteAccess, userid),
            headerWsKey,
            headerWsProtocol,
            headerWsExtensions,
            context
          )
        })
      } catch (err) {
        console.log(`Failed to auth to endpoint ${url}`, err)
        if (aborted) return
        res.cork(() => {
          res.writeStatus('401 Unauthorized').end('Unauthorized')
        })
      }
    },
    open: async (ws) => {
      const user = ws.getUserData()
      // Entry log (always-on, unlike the lib0 `log()` below) so a hang on the
      // FIRST step of the handler is still distinguishable from "open never
      // ran" in the request stream.
      console.info(`[y-redis ws.open] start room="${user.room}" uid=${user.id}`)
      log(() => ['client connected (uid=', user.id, ', ip=', Buffer.from(ws.getRemoteAddressAsText()).toString(), ')'])
      const stream = api.computeRedisRoomStreamName(user.room, 'index', redisPrefix)
      // uWS does not await the async open callback, so a throw inside the
      // body lands as an unhandled rejection on Node's microtask queue —
      // invisible without a global listener, and on some configurations
      // silently dropped. Wrap the whole body so the room context reaches
      // the logs synchronously; otherwise a per-room failure (e.g. a Yjs
      // decode throw inside getDoc/applyUpdateV2, or a downstream encode)
      // shows up at the load balancer as a 502 with no server-side trace.
      //
      // Chain-of-progress `console.info` calls flag each step's exit so the
      // last line before silence names the hang site — necessary because an
      // unsettled await (a Firestore `.get()` that never resolves, a Yjs
      // decode infinite-looping) cannot be caught by this try/catch.
      try {
        user.subs.add(stream)
        ws.subscribe(stream)
        console.info(`[y-redis ws.open] subscribed room="${user.room}"`)
        user.initialRedisSubId = subscriber.subscribe(stream, redisMessageSubscriber).redisId
        console.info(`[y-redis ws.open] redis-sub-ok room="${user.room}"`)
        // Race the async getDoc against a 10s deadline so an unsettled
        // promise (the suspected hang on `financials` / `contents-footer`)
        // is converted into a rejection that the catch below can pick up.
        // 10s is generous for a normal Firestore read (sub-100ms in
        // observed traces) and well under Cloud Run's WS idle timeout.
        const indexDoc = await withTimeout(client.getDoc(user.room, 'index'), OPEN_GET_DOC_TIMEOUT_MS, 'client.getDoc')
        console.info(`[y-redis ws.open] getDoc-ok room="${user.room}" hasContent=${indexDoc.ydoc.store.clients.size > 0}`)
        if (indexDoc.ydoc.store.clients.size === 0) {
          initDocCallback(user.room, 'index', client)
        }
        if (user.isClosed) {
          // The close callback ran while we were awaiting getDoc — surface
          // the silent return path so a `closed-before-send` line names the
          // race that aborts this open. The matching `[y-redis ws.close]`
          // line emitted by the close callback below pairs uid-wise.
          console.info(`[y-redis ws.open] closed-before-send room="${user.room}" uid=${user.id}`)
          return
        }
        ws.cork(() => {
          ws.send(protocol.encodeSyncStep1(Y.encodeStateVector(indexDoc.ydoc)), true, false)
          ws.send(protocol.encodeSyncStep2(Y.encodeStateAsUpdate(indexDoc.ydoc)), true, true)
          if (indexDoc.awareness.states.size > 0) {
            ws.send(protocol.encodeAwarenessUpdate(indexDoc.awareness, array.from(indexDoc.awareness.states.keys())), true, true)
          }
        })
        console.info(`[y-redis ws.open] sent-sync room="${user.room}"`)
        if (api.isSmallerRedisId(indexDoc.redisLastId, user.initialRedisSubId)) {
          // our subscription is newer than the content that we received from the api
          // need to renew subscription id and make sure that we catch the latest content.
          subscriber.ensureSubId(stream, indexDoc.redisLastId)
        }
        console.info(`[y-redis ws.open] done room="${user.room}"`)
      } catch (err) {
        // Narrow at the boundary: strict tsc + allowJs/checkJs would treat
        // `err` as `{}` and reject any property access without a guard.
        if (err instanceof Error) {
          console.error(`[y-redis ws.open] FAILED room="${user.room}" uid=${user.id} err=${err.message}`)
          if (err.stack) console.error(err.stack)
        } else {
          console.error(`[y-redis ws.open] FAILED room="${user.room}" uid=${user.id} err=${String(err)}`)
        }
        // Close the WS so the client and the load balancer see a clean
        // termination instead of a stalled half-open connection. Best-effort
        // — if `ws` is already torn down `end` throws and we have nothing
        // useful to do with that second error.
        try {
          ws.end(1011, 'open handler error')
        } catch (_) {}
      }
    },
    message: (ws, messageBuffer) => {
      const user = ws.getUserData()
      // don't read any messages from users without write access
      if (!user.hasWriteAccess) return
      // It is important to copy the data here
      const message = Buffer.from(messageBuffer.slice(0, messageBuffer.byteLength))
      if ( // filter out messages that we simply want to propagate to all clients
        // sync update or sync step 2
        (message[0] === protocol.messageSync && (message[1] === protocol.messageSyncUpdate || message[1] === protocol.messageSyncStep2)) ||
        // awareness update
        message[0] === protocol.messageAwareness
      ) {
        if (message[0] === protocol.messageAwareness) {
          const decoder = decoding.createDecoder(message)
          decoding.readVarUint(decoder) // read message type
          decoding.readVarUint(decoder) // read length of awareness update
          const alen = decoding.readVarUint(decoder) // number of awareness updates
          const awId = decoding.readVarUint(decoder)
          if (alen === 1 && (user.awarenessId === null || user.awarenessId === awId)) { // only update awareness if len=1
            user.awarenessId = awId
            user.awarenessLastClock = decoding.readVarUint(decoder)
          }
        }
        client.addMessage(user.room, 'index', message)
      } else if (message[0] === protocol.messageSync && message[1] === protocol.messageSyncStep1) { // sync step 1
        // can be safely ignored because we send the full initial state at the beginning
      } else {
        console.error('Unexpected message type', message)
      }
    },
    close: (ws, code, message) => {
      const user = ws.getUserData()
      // Always-on close log so we can pair a `closed-before-send` open-handler
      // line with the close code that triggered it (1000=normal client, 1001=
      // going away, 1006=abnormal/no close frame typical of proxy drops,
      // 1011=our own ws.end on open-handler error). The lib0 `log()` below
      // is gated on a runtime level and silent in production.
      const reason = message && message.byteLength ? Buffer.from(message).toString() : ''
      console.info(`[y-redis ws.close] room="${user.room}" uid=${user.id} code=${code}${reason ? ` reason="${reason}"` : ''}`)
      user.awarenessId && client.addMessage(user.room, 'index', Buffer.from(protocol.encodeAwarenessUserDisconnected(user.awarenessId, user.awarenessLastClock)))
      user.isClosed = true
      log(() => ['client connection closed (uid=', user.id, ', code=', code, ', message="', Buffer.from(message).toString(), '")'])
      user.subs.forEach(topic => {
        if (app.numSubscribers(topic) === 0) {
          subscriber.unsubscribe(topic, redisMessageSubscriber)
        }
      })
    }
  }))
  return new YWebsocketServer(app, client, subscriber)
}
