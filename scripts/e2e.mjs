#!/usr/bin/env node
/**
 * 单节点端到端验证脚本。
 *
 * 前置条件：已执行 `make up`，网关监听 http://127.0.0.1:18080。
 *
 * 覆盖 8 组、共 12 项检查：
 *   1. token + WebSocket 鉴权（非法 token 必须被握手阶段拒绝）；
 *   2. 定向消息（只投递给目标，且 payload 原样到达）；
 *   3. 房间 fan-out（Redis Set 成员展开）；
 *   4. 全量广播（所有在线用户）；
 *   5. 离线目标计数（不投递，但计入 offline_recipients）；
 *   6. 心跳续期 presence；
 *   7. 主动下线立即删除 presence；
 *   8. 崩溃/直接断开后由 presence TTL 回收。
 *
 * 每次运行使用随机后缀生成 uid/房间，避免与残留数据互相干扰。
 */

const BASE = process.env.BASE_URL || 'http://127.0.0.1:18080';
const WS_BASE = BASE.replace(/^http/, 'ws');
// 随机运行标识：附加到所有 uid/房间名后，保证本次测试数据独立。
const RUN = Math.random().toString(36).slice(2, 7);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let passed = 0;

// 记录一项通过，并打印 PASS。
function ok(label, extra = '') {
  passed++;
  console.log(`  PASS  ${label}${extra ? `  (${extra})` : ''}`);
}

// 断言条件为真，否则抛出带标签的错误；通过时调用 ok。
function assert(cond, label, extra = '') {
  if (!cond) throw new Error(`FAIL ${label}${extra ? `  (${extra})` : ''}`);
  ok(label, extra);
}

// 通过开发接口为 uid 获取 token 及运行参数。
async function token(uid) {
  const res = await fetch(`${BASE}/api/dev/token?uid=${encodeURIComponent(uid)}`);
  const body = await res.json();
  if (!res.ok) throw new Error(`token ${uid}: ${body.error || res.status}`);
  return body;
}

/**
 * 建立已认证的 WebSocket 客户端。
 *
 * 返回的 client 会自动把收到的消息解析成 JSON 放入 queue，
 * 测试通过轮询 queue 判断消息是否到达。
 */
async function connect(uid) {
  const t = await token(uid);
  const ws = new WebSocket(`${WS_BASE}/ws?token=${encodeURIComponent(t.token)}&uid=${encodeURIComponent(uid)}`);
  ws.binaryType = 'arraybuffer';
  const client = { uid, token: t.token, ws, queue: [], closed: false };
  ws.addEventListener('message', (ev) => {
    // Nchan 下发的帧可能是字符串或二进制，统一解码后尝试 JSON 解析。
    const raw = typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data);
    try { client.queue.push(JSON.parse(raw)); } catch (_) {}
  });
  ws.addEventListener('close', () => { client.closed = true; });
  // 等待握手完成：open 表示成功，error 表示被 auth_request 拒绝。
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`ws open timeout: ${uid}`)), 6000);
    ws.addEventListener('open', () => { clearTimeout(timer); resolve(); }, { once: true });
    ws.addEventListener('error', () => { clearTimeout(timer); reject(new Error(`ws handshake failed: ${uid}`)); }, { once: true });
  });
  return client;
}

/**
 * 尝试用非法 token 建立连接，返回是否被拒绝（true = 符合预期）。
 * 超时未决出结果按“未被拒绝”处理，避免测试挂死。
 */
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

/**
 * 轮询等待条件成立（每 60ms 检查一次），超时抛错。
 * 用于等待异步消息到达或 presence 状态变化。
 */
async function waitFor(fn, label, timeout = 4000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    const value = await fn();
    if (value) return value;
    await sleep(60);
  }
  throw new Error(`FAIL timeout waiting for ${label}`);
}

// 以 from 客户端身份通过 HTTP 发布消息，返回服务端统计结果。
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

// 带 Bearer token 调用 REST 接口并解析 JSON。
async function authed(path, client, method = 'GET') {
  const res = await fetch(`${BASE}${path}`, {
    method,
    headers: { Authorization: `Bearer ${client.token}` },
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`${path}: ${body.error || res.status}`);
  return body;
}

// 读取全局在线 uid 列表。
async function onlineUIDs(client) {
  const data = await authed('/api/online', client);
  return (data.online || []).map((p) => p.uid);
}

(async () => {
  console.log(`\nE2E single node  run=${RUN}  base=${BASE}\n`);
  const aliceID = `alice_${RUN}`;
  const bobID = `bob_${RUN}`;
  const roomID = `room_${RUN}`;
  const carolID = `carol_${RUN}`; // carol 始终不连接，用于离线计数

  // ---------------------------------------------------------------------------
  // 1. 鉴权：非法 token 必须在 WebSocket 握手阶段被拒绝
  // ---------------------------------------------------------------------------
  console.log('1. token + websocket auth');
  const badRejected = await expectRejected(`intruder_${RUN}`, 'not-a-valid-token');
  assert(badRejected, 'invalid token is rejected during WS handshake');

  // 两个合法客户端连接后，presence 写入 Redis，应在全局在线列表中出现。
  const alice = await connect(aliceID);
  const bob = await connect(bobID);
  ok('two clients authenticated and subscribed', `alice=${aliceID} bob=${bobID}`);

  await waitFor(async () => {
    const uids = await onlineUIDs(alice);
    return uids.includes(aliceID) && uids.includes(bobID);
  }, 'both users online');

  // ---------------------------------------------------------------------------
  // 2. 定向消息：只投递给 bob，payload 原样到达，alice 不应收到
  // ---------------------------------------------------------------------------
  console.log('2. direct message (user -> user)');
  bob.queue.length = 0;
  const direct = await publish(alice, { target_type: 'user', target_id: bobID, payload: { text: 'hello bob' } });
  assert(direct.online_recipients === 1 && direct.offline_recipients === 0, 'direct publish reports 1 online recipient');
  const gotDirect = await waitFor(() => bob.queue.find((m) => m.id === direct.msg_id), 'direct message at bob');
  assert(gotDirect.kind === 'direct' && gotDirect.from === aliceID, 'bob receives direct envelope with correct sender');
  assert(gotDirect.payload?.text === 'hello bob', 'payload is delivered byte-for-byte');
  // 给非目标用户留出接收窗口，确认确实没有消息到达。
  await sleep(400);
  assert(!alice.queue.some((m) => m.id === direct.msg_id), 'non-targeted user does not receive the direct message');

  // ---------------------------------------------------------------------------
  // 3. 房间 fan-out：成员关系由 Redis Set 维护，消息发给所有成员
  // ---------------------------------------------------------------------------
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

  // ---------------------------------------------------------------------------
  // 4. 全量广播：所有在线用户（alice + bob）都应收到
  // ---------------------------------------------------------------------------
  console.log('4. broadcast (all online users)');
  alice.queue.length = 0;
  bob.queue.length = 0;
  const broadcast = await publish(bob, { target_type: 'broadcast', payload: { text: 'hello everyone' } });
  await waitFor(() => alice.queue.find((m) => m.id === broadcast.msg_id), 'broadcast at alice');
  const gotBroadcast = await waitFor(() => bob.queue.find((m) => m.id === broadcast.msg_id), 'broadcast at bob');
  assert(gotBroadcast.kind === 'broadcast', 'broadcast envelope kind is broadcast');

  // ---------------------------------------------------------------------------
  // 5. 离线目标：carol 未连接，应计入 offline 而不是 online
  // ---------------------------------------------------------------------------
  console.log('5. offline target accounting');
  const offline = await publish(alice, { target_type: 'user', target_id: carolID, payload: { text: 'are you there' } });
  assert(offline.online_recipients === 0 && offline.offline_recipients === 1, 'offline user is counted, not delivered');

  // ---------------------------------------------------------------------------
  // 6. 心跳：续期 presence TTL，返回当前 TTL
  // ---------------------------------------------------------------------------
  console.log('6. heartbeat keeps session alive');
  const hb = await authed('/api/heartbeat', alice, 'POST');
  assert(hb.ok === true && hb.ttl_seconds > 0, 'heartbeat renews presence TTL', `ttl=${hb.ttl_seconds}s`);

  // ---------------------------------------------------------------------------
  // 7. 主动下线：/api/logout 应立即删除 presence，无需等 TTL
  // ---------------------------------------------------------------------------
  console.log('7. graceful logout removes presence immediately');
  await authed('/api/logout', alice, 'POST');
  await waitFor(async () => !(await onlineUIDs(bob)).includes(aliceID), 'alice removed after logout', 3000);
  alice.ws.close();

  // ---------------------------------------------------------------------------
  // 8. 非优雅断开：不调用 /api/logout，直接关闭 socket，
  //    只能等 presence TTL（默认 20s）过期。这里给 35s 容差。
  // ---------------------------------------------------------------------------
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
