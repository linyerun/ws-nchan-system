#!/usr/bin/env node
// Minimal load probe: N long-lived WebSockets, one broadcast, one direct hit.
// Usage: CONNECTIONS=500 node scripts/loadtest.mjs

const BASE = process.env.BASE_URL || 'http://127.0.0.1:18080';
const WS_BASE = BASE.replace(/^http/, 'ws');
const N = Number(process.env.CONNECTIONS || 200);
const CONCURRENCY = Number(process.env.CONCURRENCY || 25);
const RUN = Math.random().toString(36).slice(2, 6);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function percentile(values, p) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
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

  await sleep(1500); // let presence settle

  // Broadcast fan-out.
  const broadcastStart = performance.now();
  const bres = await fetch(`${BASE}/api/push`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${sender.token}` },
    body: JSON.stringify({ target_type: 'broadcast', payload: { text: 'load broadcast' } }),
  });
  const bdata = await bres.json();
  console.log(`broadcast api: online=${bdata.online_recipients} failed=${bdata.failed_recipients} in ${(performance.now() - broadcastStart).toFixed(0)}ms`);

  const deadline = Date.now() + 20000;
  while (arrivals.get(bdata.msg_id)?.length !== clients.length && Date.now() < deadline) {
    await sleep(30);
  }
  const broadcastArrivals = arrivals.get(bdata.msg_id) || [];
  const fanoutDone = performance.now();
  for (const a of broadcastArrivals) latencies.push(a.at - broadcastStart);
  console.log(`broadcast delivered to ${broadcastArrivals.length}/${clients.length} clients`);
  console.log(`  total ${(fanoutDone - broadcastStart).toFixed(0)}ms  p50=${percentile(latencies, 50).toFixed(0)}ms  p95=${percentile(latencies, 95).toFixed(0)}ms  p99=${percentile(latencies, 99).toFixed(0)}ms`);

  // Direct message to the last client.
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
