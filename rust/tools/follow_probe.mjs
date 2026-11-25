// Drive the Rust Host's mux socket the way the web client does, and print frames.
// usage: node follow_probe.mjs <cookie> <sessionId> [endpoint]
import { createRequire } from 'node:module'
const require = createRequire(import.meta.url)
const WebSocket = require('ws')

const [, , cookie, sessionId, endpoint = 'session/follow'] = process.argv
if (!cookie || !sessionId) {
  console.error('usage: node follow_probe.mjs <cookie> <sessionId> [endpoint]')
  process.exit(2)
}

const ws = new WebSocket('ws://127.0.0.1:3192/api/remote.mux', {
  headers: { Cookie: `dsh-auth-rs=${cookie}` },
})

const timer = setTimeout(() => {
  console.log('--- timeout, closing ---')
  ws.close()
  process.exit(0)
}, 8000)

ws.on('open', () => {
  const payload = endpoint === 'session/follow'
    ? { args: { request: {
        address: { kind: 'session', sessionId },
        activate: false,
        assistantStream: true,
        maxMessages: 500,
        turnWindow: { minMessages: 50, minTurns: 2 },
      } } }
    : { args: {} }
  ws.send(JSON.stringify({ type: 'open', streamId: 's1', endpoint, payload }))
  console.log('sent open', endpoint)
})

ws.on('message', (data) => {
  const text = data.toString()
  console.log('frame:', text)
})

ws.on('error', (error) => {
  console.error('ws error:', error.message)
  clearTimeout(timer)
  process.exit(1)
})

ws.on('close', () => {
  clearTimeout(timer)
  process.exit(0)
})
