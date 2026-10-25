#!/usr/bin/env node
// End-to-end verification for the single-node stack.
// Requires: make up (http://127.0.0.1:18080)

const BASE = process.env.BASE_URL || 'http://127.0.0.1:18080';
const WS_BASE = BASE.replace(/^http/, 'ws');
const RUN = Math.random().toString(36).slice(2, 7);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let passed = 0;
function ok(label, extra = '') {
  passed++;
  console.log(`  PASS  ${label}${extra ? `  (${extra})` : ''}`);
}
function assert(cond, label, extra = '') {
  if (!cond) throw new Error(`FAIL ${label}${extra ? `  (${extra})` : ''}`);
  ok(label, extra);
}

async function token(uid) {
  const res = await fetch(`${BASE}/api/dev/token?uid=${encodeURIComponent(uid)}`);
  const body = await res.json();
  if (!res.ok) throw new Error(`token ${uid}: ${body.error || res.status}`);
  return body;
}

async function connect(uid) {
  const t = await token(uid);
  const ws = new WebSocket(`${WS_BASE}/ws?token=${encodeURIComponent(t.token)}&uid=${encodeURIComponent(uid)}`);
  ws.binaryType = 'arraybuffer';
  const client = { uid, token: t.token, ws, queue: [], closed: false };
  ws.addEventListener('message', (ev) => {
    const raw = typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data);
    try { client.queue.push(JSON.parse(raw)); } catch (_) {}
  });
  ws.addEventListener('close', () => { client.closed = true; });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`ws open timeout: ${uid}`)), 6000);
    ws.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
    ws.addEventListener('error', () => { clearTimeout(timer); reject(new Error(`ws handshake failed: ${uid}`)); }, { once: true });
  });
  return client;
}

async function expectRejected(uid, badToken) {
  const ws = new WebSocket(`${WS_BASE}/ws?token=${encodeURIComponent(badToken)}&uid=${encodeURIComponent(uid)}`);
  return new Promise((resolve) => {
    let settled = false;
    const done = (rejected) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      try { ws.close(); } catch (_) {}
      resolve(rejected);
    };
    const timer = setTimeout(() => done(false), 5000);
    ws.addEventListener('open', () => done(false));
    ws.addEventListener('error', () => done(true));
  });
}

async function waitFor(fn, label, timeout = 4000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const value = await fn();
    if (value) return value;
    await sleep(60);
  }
  throw new Error(`FAIL timeout waiting for ${label}`);
}

async function publish(from, body) {
  const res = await fetch(`${BASE}/api/push`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${from.token}` },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`publish: ${data.error || res.status}`);
  return data;
}

async function authed(path, client, method = 'GET') {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { Authorization: `Bearer ${client.token}` },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${path}: ${body.error || res.status}`);
  return body;
}

async function onlineUIDs(client) {
  const data = await authed('/api/online', client);
  return (data.online || []).map((p) => p.uid);
}

(async () => {
  console.log(`\nE2E single node  run=${RUN}  base=${BASE}\n`);
  const aliceID = `alice_${RUN}`;
  const bobID = `bob_${RUN}`;
  const roomID = `room_${RUN}`;
  const carolID = `carol_${RUN}`;

  console.log('1. token + websocket auth');
  const badRejected = await expectRejected(`intruder_${RUN}`, 'not-a-valid-token');
  assert(badRejected, 'invalid token is rejected during WS handshake');

  const alice = await connect(aliceID);
  const bob = await connect(bobID);
  ok('two clients authenticated and subscribed', `alice=${aliceID} bob=${bobID}`);

  await waitFor(async () => {
    const uids = await onlineUIDs(alice);
    return uids.includes(aliceID) && uids.includes(bobID);
  }, 'both users online');

  console.log('2. direct message (user -> user)');
  bob.queue.length = 0;
  const direct = await publish(alice, { target_type: 'user', target_id: bobID, payload: { text: 'hello bob' } });
  assert(direct.online_recipients === 1 && direct.offline_recipients === 0, 'direct publish reports 1 online recipient');
  const gotDirect = await waitFor(() => bob.queue.find((m) => m.id === direct.msg_id), 'direct message at bob');
  assert(gotDirect.kind === 'direct' && gotDirect.from === aliceID, 'bob receives direct envelope with correct sender');
  assert(gotDirect.payload?.text === 'hello bob', 'payload is delivered byte-for-byte');
  await sleep(400);
  assert(!alice.queue.some((m) => m.id === direct.msg_id), 'non-targeted user does not receive the direct message');

  console.log('3. room fan-out (Redis Set membership)');
  await authed(`/api/rooms/${roomID}/join`, alice, 'POST');
  await authed(`/api/rooms/${roomID}/join`, bob, 'POST');
  const roomInfo = await authed(`/api/rooms/${roomID}`, alice);
  assert(roomInfo.members.length === 2, 'room has two members', `members=${roomInfo.members.map((m) => m.uid).join(',')}`);

  alice.queue.length = 0;
  bob.queue.length = 0;
  const roomMsg = await publish(alice, { target_type: 'room', target_id: roomID, payload: { text: 'room hello' } });
  await waitFor(() => alice.queue.find((m) => m.id === roomMsg.msg_id), 'room message at alice');
  const gotRoom = await waitFor(() => bob.queue.find((m) => m.id === roomMsg.msg_id), 'room message at bob');
  assert(gotRoom.kind === 'room' && gotRoom.room === roomID, 'room envelope carries room id');

  console.log('4. broadcast (all online users)');
  alice.queue.length = 0;
  bob.queue.length = 0;
  const broadcast = await publish(bob, { target_type: 'broadcast', payload: { text: 'hello everyone' } });
  await waitFor(() => alice.queue.find((m) => m.id === broadcast.msg_id), 'broadcast at alice');
  const gotBroadcast = await waitFor(() => bob.queue.find((m) => m.id === broadcast.msg_id), 'broadcast at bob');
  assert(gotBroadcast.kind === 'broadcast', 'broadcast envelope kind is broadcast');

  console.log('5. offline target accounting');
  const offline = await publish(alice, { target_type: 'user', target_id: carolID, payload: { text: 'are you there' } });
  assert(offline.online_recipients === 0 && offline.offline_recipients === 1, 'offline user is counted, not delivered');

  console.log('6. heartbeat keeps session alive');
  const hb = await authed('/api/heartbeat', alice, 'POST');
  assert(hb.ok === true && hb.ttl_seconds > 0, 'heartbeat renews presence TTL', `ttl=${hb.ttl_seconds}s`);

  console.log('7. graceful logout removes presence immediately');
  await authed('/api/logout', alice, 'POST');
  await waitFor(async () => !(await onlineUIDs(bob)).includes(aliceID), 'alice removed after logout', 3000);
  alice.ws.close();

  console.log('8. crash without logout is reclaimed by presence TTL');
  const closedAt = Date.now();
  bob.ws.close();
  await waitFor(async () => !(await onlineUIDs(bob)).includes(bobID), 'bob reclaimed by TTL', 35000);
  ok('presence TTL reclaimed the dead session', `${((Date.now() - closedAt) / 1000).toFixed(1)}s`);

  console.log(`\nALL ${passed} CHECKS PASSED\n`);
  process.exit(0);
})().catch((err) => {
  console.error(`\n${err.message}\n`);
  process.exit(1);
});
