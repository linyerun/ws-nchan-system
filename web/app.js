'use strict';

const $ = (id) => document.getElementById(id);

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

const UID_RE = /^[A-Za-z0-9_-]{1,32}$/;
const MAX_DOM_NODES = 400;

const state = {
  uid: '',
  token: '',
  nodeId: '',
  heartbeatMs: 5000,
  presenceTtl: 20,
  desired: false,
  ws: null,
  attempts: 0,
  seen: new Set(),
  online: [],
  rooms: [],
  roomInfo: {},
  timers: { heartbeat: null, online: null, rooms: null, stats: null },
  reconnectTimer: null,
};

let toastTimer = null;

/* ---------------------------------------------------------------- boot */

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

function randomUID() {
  return 'guest_' + Math.random().toString(36).slice(2, 7);
}

function bindEvents() {
  dom.connectBtn.addEventListener('click', () => {
    if (state.desired) disconnect();
    else connect();
  });

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
    if (await joinRoom(room)) {
      dom.roomInput.value = '';
      dom.targetType.value = 'room';
      onTargetTypeChange();
      dom.targetId.value = room;
    }
  });

  dom.targetType.addEventListener('change', onTargetTypeChange);

  dom.sendBtn.addEventListener('click', sendMessage);

  dom.messageInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) {
      e.preventDefault();
      sendMessage();
    }
  });
  dom.messageInput.addEventListener('input', autoSize);

  dom.clearBtn.addEventListener('click', () => dom.stream.replaceChildren());

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden && state.token) heartbeatOnce();
  });

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

async function openSocket() {
  if (!state.desired) return;
  setStatus('connecting', state.attempts > 0 ? `重连中 · 第 ${state.attempts} 次` : '连接中');

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
  if (!state.desired) return;

  state.token = meta.token;
  state.nodeId = meta.node_id || '?';
  state.heartbeatMs = (meta.heartbeat_rate || 5) * 1000;
  state.presenceTtl = meta.presence_ttl || 20;

  dom.nodeBadge.textContent = `node ${state.nodeId}`;
  dom.channelValue.textContent = `u:${state.uid}`;
  dom.ttlValue.textContent = `${state.presenceTtl}s`;
  dom.hbValue.textContent = `${meta.heartbeat_rate || 5}s`;

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

function onOpen(ws) {
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

function onClose(ev, ws) {
  if (state.ws !== ws) return;
  state.ws = null;
  stopTimers();
  setComposerEnabled(false);

  if (!state.desired) {
    setStatus('offline', '未连接');
    return;
  }
  logEvent(`连接关闭（code ${ev.code}）`, ev.code === 1000 || ev.code === 1001 ? '' : 'error');
  scheduleReconnect();
}

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

function clearReconnect() {
  if (state.reconnectTimer) {
    clearTimeout(state.reconnectTimer);
    state.reconnectTimer = null;
  }
}

function startTimers() {
  stopTimers();
  state.timers.heartbeat = setInterval(heartbeatOnce, state.heartbeatMs);
  state.timers.online = setInterval(refreshOnline, 3000);
  state.timers.stats = setInterval(refreshStats, 4000);
  state.timers.rooms = setInterval(refreshMe, 12000);
}

function stopTimers() {
  for (const key of Object.keys(state.timers)) {
    if (state.timers[key]) {
      clearInterval(state.timers[key]);
      state.timers[key] = null;
    }
  }
}

/* --------------------------------------------------------------- server */

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
    /* transient: presence TTL will decide */
  }
}

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

async function refreshMe() {
  if (!state.token) return;
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

    const env = {
      id: data.msg_id,
      kind: type === 'user' ? 'direct' : type,
      from: state.uid,
      to: type === 'user' ? targetId : type === 'broadcast' ? 'all' : undefined,
      room: type === 'room' ? targetId : undefined,
      ts: Date.now(),
      payload: { text },
    };
    if (!state.seen.has(env.id)) addMessage(env, { summary: data });
  } catch (err) {
    toast(`发送失败：${err.message}`);
    logEvent(`发送失败：${err.message}`, 'error');
  } finally {
    dom.sendBtn.disabled = !(state.token && state.ws && state.ws.readyState === WebSocket.OPEN);
    if (!dom.sendBtn.disabled) dom.messageInput.focus();
  }
}

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
    if (!env.id) env.id = `anon_${Date.now()}_${Math.random().toString(16).slice(2)}`;
    addMessage(env);
  });
}

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
    li.tabIndex = 0;

    const avatar = document.createElement('span');
    avatar.className = 'avatar';
    avatar.textContent = initial(p.uid);

    const name = document.createElement('span');
    name.className = 'name';
    name.textContent = p.uid + (p.uid === state.uid ? ' (你)' : '');

    const tag = document.createElement('span');
    tag.className = 'node-tag';
    tag.textContent = p.node_id || '?';

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
    if (type === 'room' && !dom.targetId.value && state.rooms.length) {
      dom.targetId.value = state.rooms[0];
    }
  }
  updateTargetOptions();
}

function updateTargetOptions() {
  dom.targetOptions.replaceChildren();
  const values = dom.targetType.value === 'room' ? state.rooms : state.online.map((p) => p.uid);
  for (const v of values) {
    const opt = document.createElement('option');
    opt.value = v;
    dom.targetOptions.append(opt);
  }
}

function setStatus(kind, text) {
  dom.connState.className = `pill state ${kind}`;
  dom.connState.querySelector('span').textContent = text;
}

function setComposerEnabled(on) {
  dom.messageInput.disabled = !on;
  dom.sendBtn.disabled = !on;
  dom.sendHint.textContent = on
    ? `在线状态由 ${state.heartbeatMs / 1000}s 心跳 / ${state.presenceTtl}s TTL 维护`
    : '连接后可发送消息';
  if (on) dom.messageInput.focus();
}

function logEvent(text, tone = '') {
  const node = document.createElement('div');
  node.className = `event ${tone}`;
  node.textContent = `${formatTime(Date.now())} · ${text}`;
  appendToStream(node);
}

function appendToStream(node) {
  const nearBottom = dom.stream.scrollHeight - dom.stream.scrollTop - dom.stream.clientHeight < 140;
  dom.stream.append(node);
  while (dom.stream.children.length > MAX_DOM_NODES) {
    dom.stream.firstElementChild.remove();
  }
  if (nearBottom) dom.stream.scrollTop = dom.stream.scrollHeight;
}

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

function autoSize() {
  dom.messageInput.style.height = 'auto';
  dom.messageInput.style.height = `${Math.min(120, dom.messageInput.scrollHeight)}px`;
}

function formatTime(ts) {
  const d = new Date(ts || Date.now());
  return d.toLocaleTimeString('zh-CN', { hour12: false });
}

function initial(uid) {
  const s = String(uid || '?').trim();
  return (s.charAt(0) || '?').toUpperCase();
}

function toast(text) {
  dom.toast.textContent = text;
  dom.toast.classList.add('show');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => dom.toast.classList.remove('show'), 2600);
}

init();
