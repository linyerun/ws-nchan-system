#!/usr/bin/env node
/**
 * 最小压测脚本：建立 N 条长连接，然后做一次全量广播和一次定向投递。
 *
 * 用法：
 *   CONNECTIONS=500 CONCURRENCY=50 node scripts/loadtest.mjs
 *   CONNECTIONS=300 make loadtest
 *
 * 观察指标：
 *   - 建连总耗时与失败数；
 *   - 广播的 API 耗时、到达率、端到端延迟 p50/p95/p99；
 *   - 定向消息的端到端延迟；
 *   - 批量下线耗时。
 *
 * 注意：脚本只做“最小验证”，不是严谨的基准测试；本机 Docker Desktop
 * 的资源限制、端口映射和浏览器/Node 的 WebSocket 实现都会影响结果。
 */

const BASE = process.env.BASE_URL || 'http://127.0.0.1:18080';
const WS_BASE = BASE.replace(/^http/, 'ws');
// 连接数；每条连接对应一个独立 uid 和一个 Nchan 订阅。
const N = Number(process.env.CONNECTIONS || 200);
// 建连/关闭阶段的并发度，避免瞬间打出太多 TCP 握手。
const CONCURRENCY = Number(process.env.CONCURRENCY || 25);
const RUN = Math.random().toString(36).slice(2, 6);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// 计算百分位数；values 为空时返回 0。
function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

// 为压测 uid 获取 token。
async function token(uid) {
  const res = await fetch(`${BASE}/api/dev/token?uid=${encodeURIComponent(uid)}`);
  const body = await res.json();
  if (!res.ok) throw new Error(`token ${uid}: ${body.error || res.status}`);
  return body;
}

/**
 * 建立单条 WebSocket，并记录建连耗时（openMs）。
 * 返回的 client.received 预留给按消息统计的扩展场景（当前用 arrivals 统一统计）。
 */
async function connect(uid) {
  const t = await token(uid);
  const ws = new WebSocket(`${WS_BASE}/ws?token=${encodeURIComponent(t.token)}&uid=${encodeURIComponent(uid)}`);
  ws.binaryType = 'arraybuffer';
  const started = performance.now();
  const client = { uid, token: t.token, ws, received: new Set(), openMs: 0 };
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`ws open timeout: ${uid}`)), 15000);
    ws.addEventListener('open', () => {
      clearTimeout(timer);
      client.openMs = performance.now() - started;
      resolve();
    }, { once: true });
    ws.addEventListener('error', () => { clearTimeout(timer); reject(new Error(`ws handshake failed: ${uid}`)); }, { once: true });
  });
  return client;
}

/**
 * 简单并发池：最多 limit 个 worker 同时消费 items。
 * results 按原始下标写入，保证顺序与输入一致。
 */
async function pool(items, limit, worker) {
  const results = [];
  let cursor = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (cursor < items.length) {
      const index = cursor++;
      results[index] = await worker(items[index], index);
    }
  });
  await Promise.all(runners);
  return results;
}

(async () => {
  console.log(`\nLoad test  base=${BASE}  connections=${N}\n`);
  const uids = Array.from({ length: N }, (_, i) => `load_${RUN}_${i}`);
  const t0 = performance.now();
  let failed = 0;

  // ---------------------------------------------------------------------------
  // 阶段 1：并发建连
  // ---------------------------------------------------------------------------
  const clients = [];
  await pool(uids, CONCURRENCY, async (uid) => {
    try {
      clients.push(await connect(uid));
    } catch (err) {
      failed++;
      console.error(`  connect failed: ${err.message}`);
    }
  });
  const connectMs = performance.now() - t0;
  console.log(`connected ${clients.length}/${N} in ${connectMs.toFixed(0)}ms  (failed=${failed})`);
  if (!clients.length) process.exit(1);

  // ---------------------------------------------------------------------------
  // 阶段 2：为每条连接挂消息监听，按 msg_id 记录到达者和时间
  // ---------------------------------------------------------------------------
  const sender = clients[0];
  const latencies = [];
  const arrivals = new Map();
  clients.forEach((c) => {
    c.ws.addEventListener('message', (ev) => {
      const raw = typeof ev.data === 'string' ? ev.data : new TextDecoder().decode(ev.data);
      try {
        const env = JSON.parse(raw);
        if (!arrivals.has(env.id)) arrivals.set(env.id, []);
        arrivals.get(env.id).push({ uid: c.uid, at: performance.now() });
      } catch (_) {}
    });
  });

  // 等 presence 全部写入并稳定，避免把“建连尚未完成在线注册”算作离线。
  await sleep(1500);

  // ---------------------------------------------------------------------------
  // 阶段 3：全量广播，统计到达率与端到端延迟
  // ---------------------------------------------------------------------------
  const broadcastStart = performance.now();
  const bres = await fetch(`${BASE}/api/push`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${sender.token}` },
    body: JSON.stringify({ target_type: 'broadcast', payload: { text: 'load broadcast' } }),
  });
  const bdata = await bres.json();
  console.log(`broadcast api: online=${bdata.online_recipients} failed=${bdata.failed_recipients} in ${(performance.now() - broadcastStart).toFixed(0)}ms`);

  // 最多等 20s，直到所有客户端都收到或超时。
  const deadline = Date.now() + 20000;
  while (arrivals.get(bdata.msg_id)?.length !== clients.length && Date.now() < deadline) {
    await sleep(30);
  }
  const broadcastArrivals = arrivals.get(bdata.msg_id) || [];
  const fanoutDone = performance.now();
  for (const a of broadcastArrivals) latencies.push(a.at - broadcastStart);
  console.log(`broadcast delivered to ${broadcastArrivals.length}/${clients.length} clients`);
  console.log(`  total ${(fanoutDone - broadcastStart).toFixed(0)}ms  p50=${percentile(latencies, 50).toFixed(0)}ms  p95=${percentile(latencies, 95).toFixed(0)}ms  p99=${percentile(latencies, 99).toFixed(0)}ms`);

  // ---------------------------------------------------------------------------
  // 阶段 4：定向投递给最后一条连接，验证单点路径延迟
  // ---------------------------------------------------------------------------
  const target = clients[clients.length - 1];
  const directStart = performance.now();
  const dres = await fetch(`${BASE}/api/push`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${sender.token}` },
    body: JSON.stringify({ target_type: 'user', target_id: target.uid, payload: { text: 'load direct' } }),
  });
  const ddata = await dres.json();
  let directMs = 0;
  const directDeadline = Date.now() + 10000;
  while (!(arrivals.get(ddata.msg_id) || []).some((a) => a.uid === target.uid) && Date.now() < directDeadline) {
    await sleep(10);
  }
  const hit = (arrivals.get(ddata.msg_id) || []).find((a) => a.uid === target.uid);
  if (hit) directMs = hit.at - directStart;
  console.log(`direct delivery: ${hit ? `${directMs.toFixed(0)}ms` : 'TIMEOUT'}`);

  // ---------------------------------------------------------------------------
  // 阶段 5：批量清理。先调用 /api/logout 删除 presence，再关闭 socket。
  // 这样比单纯 close 更快让在线列表归零，也验证下线接口在压力下的表现。
  // ---------------------------------------------------------------------------
  console.log(`\nclosing ${clients.length} sockets...`);
  const closeStart = performance.now();
  await pool(clients, 50, async (c) => {
    try {
      await fetch(`${BASE}/api/logout`, {
        method: 'POST',
        headers: { Authorization: `Bearer ${c.token}` },
      });
    } catch (_) {}
    c.ws.close();
  });
  console.log(`closed in ${(performance.now() - closeStart).toFixed(0)}ms\n`);
  process.exit(0);
})().catch((err) => {
  console.error(`\nload test failed: ${err.message}\n`);
  process.exit(1);
});
