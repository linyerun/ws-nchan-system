#!/usr/bin/env node
/**
 * 双节点端到端验证脚本。
 *
 * 前置条件：已执行 `make up-cluster`。
 *   node1: http://127.0.0.1:18080
 *   node2: http://127.0.0.1:18081
 *
 * 验证目标：alice 通过 node1 连接、bob 通过 node2 连接。
 * 消息不经过节点间 RPC，而是：
 *
 *   node1 发布 -> Redis PUBLISH push:node:node2
 *              -> node2 bridge -> node2 Nchan -> bob 的 WebSocket
 *
 * 共 6 项检查：节点落点、共享 presence、正向投递、反向投递、节点注册表。
 */

const BASE1 = process.env.BASE1 || 'http://127.0.0.1:18080';
const BASE2 = process.env.BASE2 || 'http://127.0.0.1:18081';
// 随机运行标识，避免与其他测试数据冲突。
const RUN = Math.random().toString(36).slice(2, 7);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let passed = 0;

// 记录一项通过。
function ok(label, extra = '') {
  passed++;
  console.log(`  PASS  ${label}${extra ? `  (${extra})` : ''}`);
}

// 断言，失败抛错终止测试。
function assert(cond, label, extra = '') {
  if (!cond) throw new Error(`FAIL ${label}${extra ? `  (${extra})` : ''}`);
  ok(label, extra);
}

// 从指定节点获取 uid 的 token；返回的 node_id 表示该 token 由哪个节点签发。
async function token(base, uid) {
  const res = await fetch(`${base}/api/dev/token?uid=${encodeURIComponent(uid)}`);
  const body = await res.json();
  if (!res.ok) throw new Error(`token ${uid}: ${body.error || res.status}`);
  return body;
}

/**
 * 通过指定节点建立 WebSocket 连接。
 * client 上额外记录 base 和 node，后续可直接复用该节点的 API。
 */
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

// 轮询等待条件成立，默认 5s 超时（跨节点多一跳 Redis + bridge，比单节点稍慢）。
async function waitFor(fn, label, timeout = 5000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const value = await fn();
    if (value) return value;
    await sleep(60);
  }
  throw new Error(`FAIL timeout waiting for ${label}`);
}

// 以某个客户端所属节点为入口发布消息。
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

// 从指定节点读取全局在线列表（数据源是共享 Redis，理论上两个节点一致）。
async function online(base, client) {
  const res = await fetch(`${base}/api/online`, { headers: { Authorization: `Bearer ${client.token}` } });
  const body = await res.json();
  return body.online || [];
}

(async () => {
  console.log(`\nE2E cluster  run=${RUN}\n  node1=${BASE1}\n  node2=${BASE2}\n`);

  // 1. 两个客户端必须分别落在不同节点上，否则后面的跨节点验证没有意义。
  const alice = await connect(BASE1, `alice_${RUN}`);
  const bob = await connect(BASE2, `bob_${RUN}`);
  assert(alice.node === 'node1' && bob.node === 'node2', 'clients landed on different nodes', `alice=${alice.node} bob=${bob.node}`);

  // 2. presence 存在共享 Redis 中，node1 应能看到 node2 上的 bob。
  await waitFor(async () => {
    const users = await online(BASE1, alice);
    return users.some((u) => u.uid === alice.uid) && users.some((u) => u.uid === bob.uid);
  }, 'shared Redis presence for both nodes');

  // 3. bob 的 presence 必须路由到 node2，而不是 token 签发节点。
  const users = await online(BASE2, bob);
  assert(users.find((u) => u.uid === bob.uid)?.node_id === 'node2', 'bob presence is routed to node2');

  // 4. 正向跨节点：alice（node1）发给 bob（node2），响应中应出现 node2。
  const direct = await publish(alice, { target_type: 'user', target_id: bob.uid, payload: { text: 'across nodes' } });
  assert(direct.nodes?.includes('node2'), 'node1 publish is routed to node2', `nodes=${(direct.nodes || []).join(',')}`);
  const got = await waitFor(() => bob.queue.find((m) => m.id === direct.msg_id), 'cross-node delivery');
  assert(got.payload?.text === 'across nodes', 'bob on node2 receives node1 message');

  // 5. 反向跨节点：bob（node2）发给 alice（node1）。
  bob.queue.length = 0;
  const back = await publish(bob, { target_type: 'user', target_id: alice.uid, payload: { text: 'reverse path' } });
  const gotBack = await waitFor(() => alice.queue.find((m) => m.id === back.msg_id), 'reverse cross-node delivery');
  assert(gotBack.payload?.text === 'reverse path', 'node2 -> node1 delivery works');

  // 6. 节点注册表：两个节点都在持续写心跳，stats.nodes 至少应有 2 条。
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
