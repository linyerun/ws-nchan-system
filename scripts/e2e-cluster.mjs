#!/usr/bin/env node
// Cross-node verification. Requires both stacks:
//   make up-cluster   (node1:18080, node2:18081)
//
// Alice connects through node1, Bob through node2. A message published on
// node1 must reach Bob via Redis Pub/Sub + node2's bridge + node2's Nchan.

const BASE1 = process.env.BASE1 || 'http://127.0.0.1:18080';
const BASE2 = process.env.BASE2 || 'http://127.0.0.1:18081';
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

async function token(base, uid) {
  const res = await fetch(`${base}/api/dev/token?uid=${encodeURIComponent(uid)}`);
  const body = await res.json();
  if (!res.ok) throw new Error(`token ${uid}: ${body.error || res.status}`);
  return body;
}

async function connect(base, uid) {
  const t = await token(base, uid);
  const wsBase = base.replace(/^http/, 'ws');
  const ws = new WebSocket(`${wsBase}/ws?token=${encodeURIComponent(t.token)}&uid=${encodeURIComponent(uid)}`);
  ws.binaryType = 'arraybuffer';
  const client = { base, uid, token: t.token, node: t.node_id, queue: [], ws };
  ws.addEventListener('message', (ev) => {
    const raw = typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data);
    try { client.queue.push(JSON.parse(raw)); } catch (_) {}
  });
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`ws open timeout: ${uid}`)), 6000);
    ws.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
    ws.addEventListener('error', () => { clearTimeout(timer); reject(new Error(`ws handshake failed: ${uid}`)); }, { once: true });
  });
  return client;
}

async function waitFor(fn, label, timeout = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const value = await fn();
    if (value) return value;
    await sleep(60);
  }
  throw new Error(`FAIL timeout waiting for ${label}`);
}

async function publish(client, body) {
  const res = await fetch(`${client.base}/api/push`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${client.token}` },
    body: JSON.stringify(body),
  });
  const data = await res.json();
  if (!res.ok) throw new Error(`publish: ${data.error || res.status}`);
  return data;
}

async function online(base, client) {
  const res = await fetch(`${base}/api/online`, { headers: { Authorization: `Bearer ${client.token}` } });
  const body = await res.json();
  return body.online || [];
}

(async () => {
  console.log(`\nE2E cluster  run=${RUN}\n  node1=${BASE1}\n  node2=${BASE2}\n`);

  const alice = await connect(BASE1, `alice_${RUN}`);
  const bob = await connect(BASE2, `bob_${RUN}`);
  assert(alice.node === 'node1' && bob.node === 'node2', 'clients landed on different nodes', `alice=${alice.node} bob=${bob.node}`);

  await waitFor(async () => {
    const users = await online(BASE1, alice);
    return users.some((u) => u.uid === alice.uid) && users.some((u) => u.uid === bob.uid);
  }, 'shared Redis presence for both nodes');

  const users = await online(BASE2, bob);
  assert(users.find((u) => u.uid === bob.uid)?.node_id === 'node2', 'bob presence is routed to node2');

  const direct = await publish(alice, { target_type: 'user', target_id: bob.uid, payload: { text: 'across nodes' } });
  assert(direct.nodes?.includes('node2'), 'node1 publish is routed to node2', `nodes=${(direct.nodes || []).join(',')}`);
  const got = await waitFor(() => bob.queue.find((m) => m.id === direct.msg_id), 'cross-node delivery');
  assert(got.payload?.text === 'across nodes', 'bob on node2 receives node1 message');

  bob.queue.length = 0;
  const back = await publish(bob, { target_type: 'user', target_id: alice.uid, payload: { text: 'reverse path' } });
  const gotBack = await waitFor(() => alice.queue.find((m) => m.id === back.msg_id), 'reverse cross-node delivery');
  assert(gotBack.payload?.text === 'reverse path', 'node2 -> node1 delivery works');

  const stats = await (await fetch(`${BASE1}/api/stats`)).json();
  assert(Array.isArray(stats.nodes) && stats.nodes.length >= 2, 'node registry sees both nodes', `nodes=${stats.nodes.map((n) => n.id).join(',')}`);

  console.log(`\nALL ${passed} CHECKS PASSED\n`);
  alice.ws.close();
  bob.ws.close();
  process.exit(0);
})().catch((err) => {
  console.error(`\n${err.message}\n`);
  process.exit(1);
});
