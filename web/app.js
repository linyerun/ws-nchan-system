'use strict';

/**
 * Nchan Push Lab 前端实验台。
 *
 * ## 职责边界
 * - 下行：只通过 WebSocket 接收服务端消息（由 nginx/Nchan 推送）；
 * - 上行：通过 HTTP `/api/push` 发送消息、通过 `/api/heartbeat` 续期在线状态；
 * - 不在 WebSocket 上实现业务协议，保持“长连接只做下行”的设计。
 *
 * ## 连接状态机
 *   offline ──点击连接──> connecting ──WS open──> online
 *      ^                       │                     │
 *      │                       └──失败/断线──────────┘
 *      └──点击断开（desired=false，不再重连）
 *
 * `state.desired` 表示“用户是否希望保持连接”，用来区分：
 *   - 用户主动断开：不重连，并调用 /api/logout 立即清理 presence；
 *   - 网络/网关异常断开：按指数退避自动重连，presence 由心跳/TTL 维护。
 *
 * ## 可靠性与去重
 * - 服务端为同一次发布的所有接收者使用同一个 `msg_id`；
 * - Nchan 会为新订阅者重放缓冲消息，重连可能收到重复帧；
 * - 本文件用 `state.seen` 按 id 去重，并限制 DOM 节点数量防止内存膨胀。
 */

const $ = (id) => document.getElementById(id);

// 集中缓存 DOM 引用，避免在热路径（消息渲染、定时刷新）中反复查询。
const dom = {
  uid: $('uidInput'),
  connectBtn: $('connectBtn'),
  connState: $('connState'),
  nodeBadge: $('nodeBadge'),
  onlineCount: $('onlineCount'),
  onlineHint: $('onlineHint'),
  onlineList: $('onlineList'),
  roomForm: $('roomForm'),
  roomInput: $('roomInput'),
  roomList: $('roomList'),
  ttlValue: $('ttlValue'),
  hbValue: $('hbValue'),
  retryValue: $('retryValue'),
  channelValue: $('channelValue'),
  stream: $('stream'),
  clearBtn: $('clearBtn'),
  targetType: $('targetType'),
  targetId: $('targetId'),
  targetLabel: $('targetLabel'),
  targetOptions: $('targetOptions'),
  messageInput: $('messageInput'),
  sendBtn: $('sendBtn'),
  sendHint: $('sendHint'),
  statPublished: $('statPublished'),
  statFanout: $('statFanout'),
  statBridge: $('statBridge'),
  statBridgeErr: $('statBridgeErr'),
  statNodes: $('statNodes'),
  toast: $('toast'),
};

// 与服务端 auth.ValidUID 一致：1-32 位字母、数字、下划线或短横线。
const UID_RE = /^[A-Za-z0-9_-]{1,32}$/;
// 消息流最多保留的 DOM 节点数。超出后从头部删除旧节点，避免长时间运行后页面卡顿。
const MAX_DOM_NODES = 400;

// 全局可变状态。这里刻意保持扁平，便于观察页面当前所处的阶段。
const state = {
  uid: '', // 当前连接使用的用户 ID
  token: '', // 当前有效的 Bearer token（也是 WebSocket 鉴权 token）
  nodeId: '', // 当前连接落在哪个 Go 节点（由 token 接口返回）
  heartbeatMs: 5000, // 心跳间隔，服务端可下发覆盖
  presenceTtl: 20, // presence TTL（秒），仅用于界面展示
  desired: false, // 用户是否希望保持连接（false = 主动断开，不重连）
  ws: null, // 当前 WebSocket 实例
  attempts: 0, // 连续重连次数，用于指数退避与展示
  seen: new Set(), // 已渲染的消息 ID，用于去重
  online: [], // 全局在线用户列表（来自 /api/online）
  rooms: [], // 我加入的房间（来自 /api/me）
  roomInfo: {}, // 房间 ID -> 房间详情（成员在线状态）
  timers: { heartbeat: null, online: null, rooms: null, stats: null },
  reconnectTimer: null,
};

let toastTimer = null;

/* ---------------------------------------------------------------- boot */

/**
 * 页面初始化：恢复 uid、绑定事件、渲染空状态并拉取一次统计。
 * uid 的优先来源：URL query > sessionStorage > 随机访客 ID。
 */
function init() {
  const params = new URLSearchParams(location.search);
  const uid = (params.get('uid') || sessionStorage.getItem('nchan.uid') || randomUID()).slice(0, 32);
  dom.uid.value = uid;
  sessionStorage.setItem('nchan.uid', uid);

  bindEvents();
  setStatus('offline', '未连接');
  setComposerEnabled(false);
  renderOnline();
  renderRooms();
  refreshStats();
  dom.uid.select();
}

// 生成随机访客 uid，用于未指定 ?uid= 时快速体验。
function randomUID() {
  return 'guest_' + Math.random().toString(36).slice(2, 7);
}

/**
 * 绑定所有 UI 事件。
 *
 * 这里集中处理三类交互：
 *   1. 连接/断开与身份输入；
 *   2. 房间加入/退出；
 *   3. 消息发送与页面生命周期（切回前台补心跳、关闭页面立即下线）。
 */
function bindEvents() {
  dom.connectBtn.addEventListener('click', () => {
    if (state.desired) disconnect();
    else connect();
  });

  // 修改 uid 后仅更新本地缓存；真正生效需要点击“连接并订阅”。
  dom.uid.addEventListener('change', () => {
    const uid = dom.uid.value.trim();
    if (UID_RE.test(uid)) sessionStorage.setItem('nchan.uid', uid);
  });

  dom.roomForm.addEventListener('submit', async (e) => {
    e.preventDefault();
    const room = dom.roomInput.value.trim();
    if (!room) return;
    if (!UID_RE.test(room)) {
      toast('房间 ID 只允许字母、数字、_、-（1-32 位）');
      return;
    }
    // 加入成功后自动切换发送目标到该房间，减少操作步骤。
    if (await joinRoom(room)) {
      dom.roomInput.value = '';
      dom.targetType.value = 'room';
      onTargetTypeChange();
      dom.targetId.value = room;
    }
  });

  dom.targetType.addEventListener('change', onTargetTypeChange);

  dom.sendBtn.addEventListener('click', sendMessage);

  // Enter 发送，Shift+Enter 换行；isComposing 用于兼容中文输入法候选状态，
  // 避免选词时按 Enter 误发送。
  dom.messageInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) {
      e.preventDefault();
      sendMessage();
    }
  });
  dom.messageInput.addEventListener('input', autoSize);

  dom.clearBtn.addEventListener('click', () => dom.stream.replaceChildren());

  // 从后台标签页回到前台时立即补一次心跳，缩短 presence 可能已过期的窗口。
  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && state.token) heartbeatOnce();
  });

  // 关闭/刷新页面前主动下线。keepalive 让请求在页面卸载过程中仍能发出；
  // 若浏览器不支持或请求失败，presence 仍会由 TTL 在 20s 内回收。
  window.addEventListener('beforeunload', () => {
    if (!state.token) return;
    fetch('/api/logout', {
      method: 'POST',
      headers: { Authorization: `Bearer ${state.token}` },
      keepalive: true,
    }).catch(() => {});
    if (state.ws) {
      try { state.ws.close(); } catch (_) {}
    }
  });
}

/* ----------------------------------------------------------- connection */

/**
 * 用户点击“连接并订阅”：
 *   1. 校验 uid；
 *   2. 标记 desired=true（允许自动重连）；
 *   3. 获取 token 并建立 WebSocket。
 */
async function connect() {
  const uid = dom.uid.value.trim();
  if (!UID_RE.test(uid)) {
    toast('用户 ID 只允许字母、数字、_、-（1-32 位）');
    return;
  }
  state.uid = uid;
  state.desired = true;
  sessionStorage.setItem('nchan.uid', uid);

  dom.uid.disabled = true;
  dom.connectBtn.textContent = '断开连接';
  dom.connectBtn.classList.remove('primary');
  dom.connectBtn.classList.add('ghost');

  logEvent(`申请 token 并为 ${uid} 建立 WebSocket`);
  await openSocket();
}

/**
 * 用户主动断开：
 *   - 清空 desired，阻止自动重连；
 *   - 停止所有定时器；
 *   - 调用 /api/logout 立即删除 presence（不用等 TTL）；
 *   - 重置界面状态。
 *
 * 注意：即使关闭 socket 失败，服务端 presence 也已被显式清理。
 */
function disconnect() {
  state.desired = false;
  clearReconnect();
  stopTimers();

  const token = state.token;
  const ws = state.ws;
  state.ws = null;
  state.token = '';

  if (ws) {
    try { ws.close(1000, 'client disconnect'); } catch (_) {}
  }
  if (token) {
    fetch('/api/logout', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
      keepalive: true,
    }).catch(() => {});
  }

  state.attempts = 0;
  state.online = [];
  state.rooms = [];
  state.roomInfo = {};
  dom.retryValue.textContent = '0';
  dom.nodeBadge.textContent = 'node —';
  dom.channelValue.textContent = '—';
  dom.uid.disabled = false;
  dom.connectBtn.textContent = '连接并订阅';
  dom.connectBtn.classList.add('primary');
  dom.connectBtn.classList.remove('ghost');

  setStatus('offline', '未连接');
  setComposerEnabled(false);
  renderOnline();
  renderRooms();
  logEvent('已断开连接，在线状态已清理');
  refreshStats();
}

/**
 * 获取 token 并建立 WebSocket。重连时也走这里。
 *
 * 流程：
 *   1. GET /api/dev/token 获取 token 与运行参数（心跳间隔、presence TTL、节点 ID）；
 *   2. 用 ws(s)://{host}/ws?token=&uid= 建立连接；
 *   3. nginx 的 auth_request 会先校验 token 并写入 presence，然后交给 Nchan 订阅。
 *
 * 任何一步失败都进入指数退避重连，而不是立即高频重试。
 */
async function openSocket() {
  if (!state.desired) return;
  setStatus('connecting', state.attempts > 0 ? `重连中 · 第 ${state.attempts} 次` : '连接中');

  // 1) 换取 token。开发模式下由 Go 直接签发；生产可替换为业务鉴权接口。
  let meta;
  try {
    const res = await fetch(`/api/dev/token?uid=${encodeURIComponent(state.uid)}`, { cache: 'no-store' });
    meta = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(meta.error || `HTTP ${res.status}`);
  } catch (err) {
    logEvent(`获取 token 失败：${err.message}`, 'error');
    scheduleReconnect();
    return;
  }
  // 请求 token 期间用户可能已点击断开，这里再检查一次避免“断开后又连上”。
  if (!state.desired) return;

  state.token = meta.token;
  state.nodeId = meta.node_id || '?';
  state.heartbeatMs = (meta.heartbeat_rate || 5) * 1000;
  state.presenceTtl = meta.presence_ttl || 20;

  dom.nodeBadge.textContent = `node ${state.nodeId}`;
  dom.channelValue.textContent = `u:${state.uid}`;
  dom.ttlValue.textContent = `${state.presenceTtl}s`;
  dom.hbValue.textContent = `${meta.heartbeat_rate || 5}s`;

  // 2) 建立 WebSocket。浏览器 WebSocket 无法设置自定义 Header，
  // 因此 token 放在 query string 中；nginx 日志格式已刻意去掉 query。
  const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
  const url = `${scheme}://${location.host}/ws?token=${encodeURIComponent(state.token)}&uid=${encodeURIComponent(state.uid)}`;

  let ws;
  try {
    ws = new WebSocket(url);
  } catch (err) {
    logEvent(`WebSocket 创建失败：${err.message}`, 'error');
    scheduleReconnect();
    return;
  }
  ws.binaryType = 'arraybuffer';
  state.ws = ws;

  ws.onopen = () => onOpen(ws);
  ws.onmessage = onMessage;
  ws.onerror = () => logEvent('WebSocket 发生错误', 'error');
  ws.onclose = (ev) => onClose(ev, ws);
}

/**
 * WebSocket 建连成功回调：
 *   - 重置重连计数；
 *   - 启动心跳、在线列表、统计、房间刷新四个定时器；
 *   - 立即补一次心跳并自动加入 lobby。
 */
function onOpen(ws) {
  // 防止旧连接的 open 事件污染新连接状态。
  if (state.ws !== ws) return;
  state.attempts = 0;
  dom.retryValue.textContent = '0';
  setStatus('online', '在线');
  setComposerEnabled(true);
  logEvent(`已连接 · 通道 u:${state.uid} · 节点 ${state.nodeId}`);

  startTimers();
  heartbeatOnce();
  refreshOnline();
  refreshStats();
  ensureLobby();
}

/**
 * WebSocket 关闭回调。
 *
 * 主动断开（desired=false）时只更新界面；
 * 非预期关闭时记录日志并按退避策略重连。
 */
function onClose(ev, ws) {
  if (state.ws !== ws) return;
  state.ws = null;
  stopTimers();
  setComposerEnabled(false);

  if (!state.desired) {
    setStatus('offline', '未连接');
    return;
  }
  // 1000/1001 是正常关闭，不算错误；其他 code 用错误样式提示。
  logEvent(`连接关闭（code ${ev.code}）`, ev.code === 1000 || ev.code === 1001 ? '' : 'error');
  scheduleReconnect();
}

/**
 * 指数退避重连：
 *   delay = min(10s, 700ms * 2^(attempts-1)) * [0.8, 1.2) 抖动
 *
 * 抖动是为了避免多个客户端在网关重启后同一时刻集中重连（惊群）。
 */
function scheduleReconnect() {
  if (!state.desired) return;
  clearReconnect();
  state.attempts += 1;
  dom.retryValue.textContent = String(state.attempts);

  const base = Math.min(10000, 700 * Math.pow(2, state.attempts - 1));
  const delay = Math.round(base * (0.8 + Math.random() * 0.4));
  setStatus('connecting', `重连中 · ${Math.ceil(delay / 1000)}s`);
  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = null;
    openSocket();
  }, delay);
}

// 取消尚未执行的重连定时器（主动断开或再次调度前调用）。
function clearReconnect() {
  if (state.reconnectTimer) {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }
}

/**
 * 启动在线期间的周期任务：
 *   - 心跳：续期 presence，防止被判定离线；
 *   - 在线列表：每 3s 刷新；
 *   - 统计：每 4s 刷新；
 *   - 我的房间：每 12s 刷新（成员在线状态变化较慢）。
 */
function startTimers() {
  stopTimers();
  state.timers.heartbeat = setInterval(heartbeatOnce, state.heartbeatMs);
  state.timers.online = setInterval(refreshOnline, 3000);
  state.timers.stats = setInterval(refreshStats, 4000);
  state.timers.rooms = setInterval(refreshMe, 12000);
}

// 停止并清空所有周期任务，连接关闭或重新建连时调用。
function stopTimers() {
  for (const key of Object.keys(state.timers)) {
    if (state.timers[key]) {
      clearInterval(state.timers[key]);
      state.timers[key] = null;
    }
  }
}

/* --------------------------------------------------------------- server */

/**
 * 发送一次心跳，续期服务端 presence TTL。
 *
 * 如果返回 401，说明 token 已过期或被替换：主动关闭 socket，
 * 让 onClose 进入重连流程，重新获取 token。
 * 网络错误静默忽略——presence TTL 会决定最终状态，连续失败自然会离线。
 */
async function heartbeatOnce() {
  if (!state.token) return;
  try {
    const res = await fetch('/api/heartbeat', {
      method: 'POST',
      headers: { Authorization: `Bearer ${state.token}` },
    });
    if (res.status === 401) {
      logEvent('token 已失效，重新建立连接', 'error');
      if (state.ws) state.ws.close();
    }
  } catch (_) {
    /* 瞬时网络错误忽略，由 presence TTL 决定最终状态 */
  }
}

// 拉取全局在线用户列表并刷新左侧栏；失败时保留上一次成功的结果。
async function refreshOnline() {
  if (!state.token) return;
  try {
    const res = await fetch('/api/online', {
      headers: { Authorization: `Bearer ${state.token}` },
      cache: 'no-store',
    });
    if (!res.ok) return;
    const data = await res.json();
    state.online = data.online || [];
    renderOnline();
  } catch (_) {}
}

/**
 * 拉取“我的房间”及每个房间的成员在线详情。
 *
 * 先请求 /api/me 得到房间列表，再并发请求每个房间的详情；
 * 某个房间请求失败时只丢弃该房间的详情，不影响整体刷新。
 */
async function refreshMe() {
  if (!state.token) return;
  // 固定本次请求使用的 token 快照，避免请求过程中 token 被重置导致串号。
  const token = state.token;
  try {
    const res = await fetch('/api/me', { headers: { Authorization: `Bearer ${token}` }, cache: 'no-store' });
    if (!res.ok) return;
    const me = await res.json();
    state.rooms = me.rooms || [];

    const details = await Promise.all(state.rooms.map(async (room) => {
      try {
        const r = await fetch(`/api/rooms/${encodeURIComponent(room)}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        return r.ok ? await r.json() : null;
      } catch (_) {
        return null;
      }
    }));

    state.roomInfo = {};
    for (const d of details) {
      if (d) state.roomInfo[d.room] = d;
    }
    renderRooms();
    updateTargetOptions();
  } catch (_) {}
}

/**
 * 拉取节点统计并更新底部状态栏。
 * /api/stats 无需鉴权，即使未连接也会刷新。
 */
async function refreshStats() {
  try {
    const res = await fetch('/api/stats', { cache: 'no-store' });
    if (!res.ok) return;
    const d = await res.json();
    dom.statPublished.textContent = d.published ?? 0;
    dom.statFanout.textContent = d.fanout_targets ?? 0;
    dom.statBridge.textContent = d.bridge_delivered ?? 0;
    dom.statBridgeErr.textContent = d.bridge_errors ?? 0;
    const nodes = d.nodes || [];
    dom.statNodes.textContent = String(nodes.length);
    dom.statNodes.title = nodes.map((n) => `${n.id}(${n.online})`).join(', ');
  } catch (_) {}
}

/**
 * 连接成功后自动加入默认房间 lobby，方便两个浏览器窗口立刻互发房间消息。
 * 已加入时不重复提示；接口本身是幂等的（Set 添加）。
 */
async function ensureLobby() {
  if (!state.token) return;
  const token = state.token;
  const known = state.rooms.includes('lobby');
  try {
    const res = await fetch('/api/rooms/lobby/join', {
      method: 'POST',
      headers: { Authorization: `Bearer ${token}` },
    });
    if (!res.ok) return;
    await refreshMe();
    if (!known) logEvent('已自动加入 lobby 房间');
  } catch (_) {}
}

// 加入指定房间；成功返回 true，失败弹出 toast 并返回 false。
async function joinRoom(room) {
  if (!state.token) {
    toast('请先连接');
    return false;
  }
  try {
    const res = await fetch(`/api/rooms/${encodeURIComponent(room)}/join`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${state.token}` },
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
    logEvent(`已加入房间 ${room}`);
    await refreshMe();
    return true;
  } catch (err) {
    toast(`加入房间失败：${err.message}`);
    return false;
  }
}

// 退出指定房间；如果当前发送目标正是该房间，同时清空目标输入框。
async function leaveRoom(room) {
  if (!state.token) return;
  try {
    const res = await fetch(`/api/rooms/${encodeURIComponent(room)}/leave`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${state.token}` },
    });
    if (!res.ok) return;
    logEvent(`已退出房间 ${room}`);
    if (dom.targetType.value === 'room' && dom.targetId.value === room) dom.targetId.value = '';
    await refreshMe();
  } catch (_) {}
}

/* --------------------------------------------------------------- sending */

/**
 * 通过 HTTP 上行发送消息。
 *
 * 关键点：
 *   - 发送前要求 WebSocket 已打开：虽然上行走 HTTP，但需要确保自己在线，
 *     否则消息能发出去却收不到别人的回复，交互上容易困惑；
 *   - 目标校验与服务端保持一致；
 *   - 发送成功后立即在本地渲染一条“自己的消息”（乐观渲染），
 *     并把自己的 msg_id 加入去重表，避免稍后 Nchan 把同一消息再推回来时重复显示。
 */
async function sendMessage() {
  const text = dom.messageInput.value.trim();
  if (!text) return;
  if (!state.token || !state.ws || state.ws.readyState !== WebSocket.OPEN) {
    toast('连接未就绪');
    return;
  }

  const type = dom.targetType.value;
  const targetId = dom.targetId.value.trim();
  if (type !== 'broadcast') {
    if (!targetId) {
      toast(type === 'user' ? '请选择或输入用户 ID' : '请选择房间');
      return;
    }
    if (!UID_RE.test(targetId)) {
      toast('目标 ID 格式不正确');
      return;
    }
  }

  dom.sendBtn.disabled = true;
  try {
    const res = await fetch('/api/push', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${state.token}`,
      },
      body: JSON.stringify({
        target_type: type,
        target_id: type === 'broadcast' ? '' : targetId,
        payload: { text },
      }),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);

    dom.messageInput.value = '';
    autoSize();

    // 用服务端返回的 msg_id 构造本地信封，与服务端下发的格式保持一致。
    const env = {
      id: data.msg_id,
      kind: type === 'user' ? 'direct' : type,
      from: state.uid,
      to: type === 'user' ? targetId : type === 'broadcast' ? 'all' : undefined,
      room: type === 'room' ? targetId : undefined,
      ts: Date.now(),
      payload: { text },
    };
    // seen 检查 + addMessage 内部再次检查，保证本地渲染与后续下行不重复。
    if (!state.seen.has(env.id)) addMessage(env, { summary: data });
  } catch (err) {
    toast(`发送失败：${err.message}`);
    logEvent(`发送失败：${err.message}`, 'error');
  } finally {
    // 只有连接仍可用时才恢复发送按钮，避免把按钮错误地重新启用。
    dom.sendBtn.disabled = !(state.token && state.ws && state.ws.readyState === WebSocket.OPEN);
    if (!dom.sendBtn.disabled) dom.messageInput.focus();
  }
}

/**
 * WebSocket 下行消息回调。
 *
 * 帧可能是字符串、Blob 或 ArrayBuffer（binaryType 被设为 arraybuffer），
 * 先统一解码成文本，再按 JSON 解析成 Envelope。
 * 非 JSON 帧通常来自代理错误页或调试消息，只记日志，不渲染为业务消息。
 */
function onMessage(ev) {
  decodeFrame(ev.data).then((raw) => {
    if (raw === null) return;
    let env;
    try {
      env = JSON.parse(raw);
    } catch (_) {
      logEvent(`收到非 JSON 帧：${String(raw).slice(0, 120)}`, 'error');
      return;
    }
    if (!env || typeof env !== 'object') return;
    // 理论上服务端总会带 id；兜底生成一个，保证去重逻辑有键可用。
    if (!env.id) env.id = `anon_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    addMessage(env);
  });
}

// 把 WebSocket 帧统一解码为字符串；失败返回 null 表示丢弃该帧。
async function decodeFrame(data) {
  try {
    if (typeof data === 'string') return data;
    if (data instanceof Blob) return await data.text();
    if (data instanceof ArrayBuffer) return new TextDecoder('utf-8').decode(data);
    return String(data);
  } catch (_) {
    return null;
  }
}

/* --------------------------------------------------------------- render */

/**
 * 把一条消息渲染进消息流。
 *
 * 安全：所有用户可控内容都通过 textContent 写入，绝不使用 innerHTML，
 * 避免 XSS。payload 不是 {text} 结构时，以格式化 JSON 展示。
 *
 * @param {object} env 服务端 Envelope（或本地构造的同构对象）
 * @param {{summary?: object}} opts summary 用于发送方展示投递统计
 */
function addMessage(env, opts = {}) {
  if (seenBefore(env.id)) return;

  const mine = env.from === state.uid;
  const wrap = document.createElement('div');
  wrap.className = `msg ${mine ? 'out' : 'in'}`;

  const avatar = document.createElement('div');
  avatar.className = 'avatar';
  avatar.textContent = initial(env.from);

  const body = document.createElement('div');
  body.className = 'body';

  const meta = document.createElement('div');
  meta.className = 'meta';

  const author = document.createElement('span');
  author.className = 'author';
  author.textContent = (env.from || 'unknown') + (mine ? ' (你)' : '');

  const kind = document.createElement('span');
  kind.className = `kind ${env.kind || 'direct'}`;
  if (env.kind === 'room') kind.textContent = `room:${env.room || ''}`;
  else if (env.kind === 'broadcast') kind.textContent = 'broadcast';
  else kind.textContent = 'direct';

  const time = document.createElement('span');
  time.className = 'time';
  time.textContent = formatTime(env.ts);

  meta.append(author, kind, time);
  body.append(meta);

  const textWrap = document.createElement('div');
  textWrap.className = 'text';
  const payload = env.payload && typeof env.payload === 'object' ? env.payload : {};
  if (typeof payload.text === 'string') {
    textWrap.textContent = payload.text;
  } else {
    const pre = document.createElement('pre');
    pre.textContent = JSON.stringify(payload, null, 2);
    textWrap.append(pre);
  }
  body.append(textWrap);

  // 发送方额外展示服务端统计：在线/离线/失败接收者数量。
  if (opts.summary) {
    const foot = document.createElement('div');
    foot.className = 'foot';
    foot.textContent =
      `已发送 · 在线 ${opts.summary.online_recipients ?? 0} · 离线 ${opts.summary.offline_recipients ?? 0}` +
      (opts.summary.failed_recipients ? ` · 失败 ${opts.summary.failed_recipients}` : '');
    body.append(foot);
  }

  wrap.append(avatar, body);
  appendToStream(wrap);
}

// 渲染在线用户列表；自己排在最前，其余按 uid 字典序。
function renderOnline() {
  dom.onlineList.replaceChildren();
  const list = [...state.online].sort((a, b) => {
    if (a.uid === state.uid) return -1;
    if (b.uid === state.uid) return 1;
    return a.uid.localeCompare(b.uid);
  });

  dom.onlineCount.textContent = `${list.length} 在线`;
  dom.onlineHint.textContent = state.token ? `${list.length} 个活跃 session` : '—';

  if (!list.length) {
    const li = document.createElement('li');
    li.className = 'empty';
    li.textContent = state.token ? '暂无在线用户' : '连接后显示';
    dom.onlineList.append(li);
    updateTargetOptions();
    return;
  }

  for (const p of list) {
    const li = document.createElement('li');
    li.className = 'user-item' + (p.uid === state.uid ? ' self' : '');
    li.tabIndex = 0; // 可聚焦，支持键盘选择

    const avatar = document.createElement('span');
    avatar.className = 'avatar';
    avatar.textContent = initial(p.uid);

    const name = document.createElement('span');
    name.className = 'name';
    name.textContent = p.uid + (p.uid === state.uid ? ' (你)' : '');

    const tag = document.createElement('span');
    tag.className = 'node-tag';
    tag.textContent = p.node_id || '?';

    // 点击某个在线用户，自动把发送目标切换为“定向该用户”。
    const pick = () => {
      dom.targetType.value = 'user';
      onTargetTypeChange();
      dom.targetId.value = p.uid;
      dom.messageInput.focus();
    };
    li.addEventListener('click', pick);
    li.addEventListener('keydown', (e) => {
      if (e.key === 'Enter') pick();
    });

    li.append(avatar, name, tag);
    dom.onlineList.append(li);
  }
  updateTargetOptions();
}

// 渲染“我的房间”列表：显示在线人数/总成员数，并提供退出按钮。
function renderRooms() {
  dom.roomList.replaceChildren();

  if (!state.rooms.length) {
    const li = document.createElement('li');
    li.className = 'empty';
    li.textContent = state.token ? '尚未加入房间，加入 lobby 开始' : '尚未加入房间';
    dom.roomList.append(li);
    updateTargetOptions();
    return;
  }

  for (const room of state.rooms) {
    const info = state.roomInfo[room];
    const members = info ? info.members || [] : [];
    const online = members.filter((m) => m.online).length;

    const li = document.createElement('li');
    li.className = 'room-item';

    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'room-btn';
    const dot = document.createElement('span');
    dot.className = 'dot';
    const name = document.createElement('span');
    name.className = 'room-name';
    name.textContent = room;
    btn.append(dot, name);
    // 点击房间名，把发送目标切换为该房间。
    btn.addEventListener('click', () => {
      dom.targetType.value = 'room';
      onTargetTypeChange();
      dom.targetId.value = room;
      dom.messageInput.focus();
    });

    const count = document.createElement('span');
    count.className = 'room-count';
    count.textContent = `${online}/${members.length}`;

    const leave = document.createElement('button');
    leave.type = 'button';
    leave.className = 'leave-btn';
    leave.textContent = '退出';
    leave.addEventListener('click', () => leaveRoom(room));

    li.append(btn, count, leave);
    dom.roomList.append(li);
  }
  updateTargetOptions();
}

// 根据“推送目标”下拉框切换输入框的标签、占位符与禁用状态。
function onTargetTypeChange() {
  const type = dom.targetType.value;
  if (type === 'broadcast') {
    dom.targetLabel.textContent = '广播范围';
    dom.targetId.value = '';
    dom.targetId.disabled = true;
    dom.targetId.placeholder = '所有在线用户';
  } else {
    dom.targetLabel.textContent = type === 'user' ? '用户 ID' : '房间 ID';
    dom.targetId.disabled = false;
    dom.targetId.placeholder = type === 'user' ? 'bob' : 'lobby';
    // 切到房间模式且输入为空时，默认选中第一个已加入的房间。
    if (type === 'room' && !dom.targetId.value && state.rooms.length) {
      dom.targetId.value = state.rooms[0];
    }
  }
  updateTargetOptions();
}

// 更新输入框的 datalist 候选：用户模式用在线 uid，房间模式用我的房间。
function updateTargetOptions() {
  dom.targetOptions.replaceChildren();
  const values = dom.targetType.value === 'room' ? state.rooms : state.online.map((p) => p.uid);
  for (const v of values) {
    const opt = document.createElement('option');
    opt.value = v;
    dom.targetOptions.append(opt);
  }
}

// 更新顶部连接状态胶囊（online / connecting / offline）。
function setStatus(kind, text) {
  dom.connState.className = `pill state ${kind}`;
  dom.connState.querySelector('span').textContent = text;
}

// 控制消息输入区是否可用，并展示在线状态维护参数说明。
function setComposerEnabled(on) {
  dom.messageInput.disabled = !on;
  dom.sendBtn.disabled = !on;
  dom.sendHint.textContent = on
    ? `在线状态由 ${state.heartbeatMs / 1000}s 心跳 / ${state.presenceTtl}s TTL 维护`
    : '连接后可发送消息';
  if (on) dom.messageInput.focus();
}

// 在消息流中插入一条系统事件（连接、错误、加入房间等），区别于用户消息。
function logEvent(text, tone = '') {
  const node = document.createElement('div');
  node.className = `event ${tone}`;
  node.textContent = `${formatTime(Date.now())} · ${text}`;
  appendToStream(node);
}

/**
 * 向消息流追加节点，并做两件事：
 *   1. 超过 MAX_DOM_NODES 时从头部删除最旧的节点，控制 DOM 规模；
 *   2. 如果用户原本就在底部附近，追加后自动滚到底部；
 *      如果用户正在向上翻历史消息，则不打断阅读。
 */
function appendToStream(node) {
  const nearBottom = dom.stream.scrollHeight - dom.stream.scrollTop - dom.stream.clientHeight < 140;
  dom.stream.append(node);
  while (dom.stream.children.length > MAX_DOM_NODES) {
    dom.stream.firstElementChild.remove();
  }
  if (nearBottom) dom.stream.scrollTop = dom.stream.scrollHeight;
}

/**
 * 消息 ID 去重。
 *
 * Nchan 会为新订阅者重放缓冲消息，重连/刷新后可能再次收到旧消息；
 * 服务端也可能因 Nchan 重试产生重复帧。这里用 Set 记录已渲染 ID。
 * 超过 2000 条时批量淘汰最早的 500 条（Set 保持插入顺序），
 * 避免长时间运行导致内存无限增长。
 */
function seenBefore(id) {
  if (state.seen.has(id)) return true;
  state.seen.add(id);
  if (state.seen.size > 2000) {
    let removed = 0;
    for (const v of state.seen) {
      state.seen.delete(v);
      if (++removed >= 500) break;
    }
  }
  return false;
}

// 根据内容自动调整输入框高度，最大 120px。
function autoSize() {
  dom.messageInput.style.height = 'auto';
  dom.messageInput.style.height = `${Math.min(120, dom.messageInput.scrollHeight)}px`;
}

// 把毫秒时间戳格式化为 24 小时制本地时间（HH:mm:ss）。
function formatTime(ts) {
  const d = new Date(ts || Date.now());
  return d.toLocaleTimeString('zh-CN', { hour12: false });
}

// 取 uid 首字符作为头像文本。
function initial(uid) {
  const s = String(uid || '?').trim();
  return (s.charAt(0) || '?').toUpperCase();
}

// 底部轻提示，2.6s 后自动消失；重复调用会重置计时。
function toast(text) {
  dom.toast.textContent = text;
  dom.toast.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => dom.toast.classList.remove('show'), 2600);
}

init();
