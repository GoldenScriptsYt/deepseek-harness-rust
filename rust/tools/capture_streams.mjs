// Record the frames the TypeScript Host sends on mux streams, for porting them to Rust.
//
//   node capture_streams.mjs <origin> <token> <outfile> endpoint[=payloadJson] ...
//
// Run from anywhere; `ws` is resolved from packages/api/gateway. Trigger the change you want
// to observe (for example an RPC with curl) while this runs; it listens for 4.5 seconds.
import { createRequire } from 'node:module'
import { writeFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const repo = join(dirname(fileURLToPath(import.meta.url)), '..', '..')
const require = createRequire(join(repo, 'packages', 'api', 'gateway', 'package.json'))
const WebSocket = require('ws')

const [origin, token, outFile, ...specs] = process.argv.slice(2)
const jar = await fetch(`${origin}/?token=${token}`, { redirect: 'manual' })
const cookie = jar.headers.get('set-cookie').split(';')[0]

const ws = new WebSocket(origin.replace('http', 'ws') + '/api/remote.mux', { headers: { cookie } })
const frames = {}
ws.on('message', (data) => {
  const m = JSON.parse(data.toString())
  ;(frames[m.streamId] ??= []).push(m)
})
await new Promise((res, rej) => { ws.on('open', res); ws.on('error', rej) })

const open = (id, endpoint, payload) => ws.send(JSON.stringify({ type: 'open', streamId: id, endpoint, payload }))
open('events', '$events', { args: {} })
const ids = {}
specs.forEach((spec, i) => {
  const [endpoint, json] = spec.split(/=(.*)/s)
  ids[`s${i}`] = endpoint
  open(`s${i}`, endpoint, json ? JSON.parse(json) : { args: {} })
})
await new Promise(r => setTimeout(r, 4500))
const out = { events: frames.events ?? [] }
for (const [id, endpoint] of Object.entries(ids)) out[endpoint] = frames[id] ?? []
writeFileSync(outFile, JSON.stringify(out, null, 1))
for (const [k, v] of Object.entries(out)) {
  console.log(k, v.length, 'frames;', v.map(f => f.type + (f.value ? ':' + JSON.stringify(f.value).slice(0, 110) : '')).join(' | ').slice(0, 420))
}
ws.close()
