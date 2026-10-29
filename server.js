// Night Market Brawl: serves the game and relays online presence over WebSocket.
// No accounts needed. Run: npm install && npm start  (default port 3000)
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const MAX_MSG = 8 * 1024;          // bytes per message
const MAX_ROOMS_PER_SOCKET = 8;
const MAX_PEERS_PER_ROOM = 200;
const MAX_MSGS_PER_SEC = 60;

const INDEX = path.join(__dirname, 'index.html');

/* ---------- 4-digit save codes ---------- */
// Storage: set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN (free at upstash.com) to keep saves
// forever. Without them, saves go in a file (DATA_DIR/saves.json), which free hosts may wipe on restart.
const SAVE_MAX = 256 * 1024;
const KEY_RE = /^brawl-[a-z0-9_-]{1,40}$/i;
const SKIP = new Set(['brawl-adm', 'brawl-savecode', 'brawl-server']);
function cleanSave(d) {
  if (!d || typeof d !== 'object' || Array.isArray(d)) return null;
  const o = {}; let tot = 0;
  for (const k of Object.keys(d)) {
    if (!KEY_RE.test(k) || SKIP.has(k.toLowerCase())) continue;
    const v = d[k];
    if (typeof v !== 'string' || v.length > 120000) continue;
    tot += k.length + v.length;
    if (tot > SAVE_MAX) return null;
    o[k] = v;
  }
  return o;
}
const Store = (() => {
  const mem = new Map();
  const up = process.env.UPSTASH_REDIS_REST_URL, tok = process.env.UPSTASH_REDIS_REST_TOKEN;
  const file = path.join(process.env.DATA_DIR || path.join(__dirname, 'data'), 'saves.json');
  if (!up) { try { const j = JSON.parse(fs.readFileSync(file, 'utf8')); for (const k in j) mem.set(k, j[k]); } catch {} }
  let t = null;
  const writeNow = () => { try { fs.mkdirSync(path.dirname(file), { recursive: true }); fs.writeFileSync(file, JSON.stringify(Object.fromEntries(mem))); } catch {} };
  const flush = () => { clearTimeout(t); t = setTimeout(writeNow, 500); };
  for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { if (!up) writeNow(); process.exit(0); });
  const cmd = async a => {
    const r = await fetch(up, { method: 'POST', headers: { Authorization: 'Bearer ' + tok, 'Content-Type': 'application/json' }, body: JSON.stringify(a) });
    const j = await r.json(); if (j.error) throw new Error(j.error); return j.result;
  };
  return {
    persistent: !!up,
    async get(c) { if (up) { const v = await cmd(['GET', 'nmb:' + c]); return v ? JSON.parse(v) : null; } return mem.get(c) || null; },
    async create(c, rec) { if (up) return (await cmd(['SET', 'nmb:' + c, JSON.stringify(rec), 'NX'])) === 'OK'; if (mem.has(c)) return false; mem.set(c, rec); flush(); return true; },
    async put(c, rec) { if (up) { await cmd(['SET', 'nmb:' + c, JSON.stringify(rec)]); return; } mem.set(c, rec); flush(); }
  };
})();
const buckets = new Map();
function hit(key, limit, windowMs) {
  const now = Date.now(); let a = buckets.get(key); if (!a) buckets.set(key, a = []);
  while (a.length && now - a[0] > windowMs) a.shift();
  if (a.length >= limit) return false; a.push(now); return true;
}
function count(key, windowMs) { const a = buckets.get(key); if (!a) return 0; const now = Date.now(); while (a.length && now - a[0] > windowMs) a.shift(); return a.length; }
setInterval(() => { const now = Date.now(); for (const [k, a] of buckets) { while (a.length && now - a[0] > 3600000) a.shift(); if (!a.length) buckets.delete(k); } }, 60000).unref();
const clientIp = req => { const x = String(req.headers['x-forwarded-for'] || '').split(',').map(s => s.trim()).filter(Boolean); return x.length ? x[x.length - 1] : req.socket.remoteAddress || 'x'; };
const FAIL_LIMIT = 10; // wrong or unknown codes allowed per IP per hour
function sendJson(res, code, obj) { res.writeHead(code, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(obj)); }
function readBody(req) {
  return new Promise((resolve, reject) => {
    let n = 0; const chunks = [];
    req.on('data', c => { n += c.length; if (n > SAVE_MAX + 4096) { reject(Object.assign(new Error('big'), { status: 413 })); req.destroy(); } else chunks.push(c); });
    req.on('end', () => { try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); } catch { reject(Object.assign(new Error('json'), { status: 400 })); } });
    req.on('error', reject);
  });
}

/* ---------- weekly tournament ---------- */
// Registration Monday to Tuesday (UTC), then one bracket round per day. Players play their match in a
// private room any time in the round window and both report the result. Needs the same storage as save codes.
const DAYMS = 86400000;
const TC = {
  regMs: +process.env.TOUR_REG_MS || 2 * DAYMS, roundMs: +process.env.TOUR_ROUND_MS || DAYMS, weekMs: +process.env.TOUR_WEEK_MS || 7 * DAYMS,
  anchor: process.env.TOUR_ANCHOR ? +process.env.TOUR_ANCHOR : 0, min: +process.env.TOUR_MIN || 4, max: 16, test: process.env.TOUR_TEST === '1'
};
const REWARD = { champion: 500, finalist: 250, semi: 120, entered: 40 };
const tnow = req => (TC.test && req && req.headers['x-test-now']) ? +req.headers['x-test-now'] : Date.now();
function tStart(now) {
  if (TC.anchor) return TC.anchor + Math.floor((now - TC.anchor) / TC.weekMs) * TC.weekMs;
  const d = new Date(now), day = (d.getUTCDay() + 6) % 7;
  return Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()) - day * DAYMS;
}
const rs = (t, r) => t.start + TC.regMs + r * TC.roundMs, re = (t, r) => rs(t, r) + TC.roundMs;
const pidOf = tok => require('crypto').createHash('sha256').update(String(tok)).digest('hex').slice(0, 12);
const tokOk = tok => typeof tok === 'string' && /^[A-Za-z0-9]{16,64}$/.test(tok);
async function tLoad(now) {
  // One current tournament at a time. A finished one is replaced when a new week begins. If too few
  // players have joined when registration ends, registration simply stays open for another period.
  const ptr = await Store.get('tour:cur'); let t = ptr ? await Store.get('tour:' + ptr.id) : null;
  const wk = tStart(now);
  if (!t || ((t.status === 'done' || t.status === 'cancelled') && wk > t.wk)) {
    const nt = { id: 't' + wk, start: wk, wk, status: 'reg', players: {}, bracket: null, claimed: {}, n: 0, prevId: t ? t.id : '' };
    await Store.put('tour:' + nt.id, nt); await Store.put('tour:cur', { id: nt.id }); t = nt;
  }
  return t;
}
const tSave = t => Store.put('tour:' + t.id, t);
const seedOf = (t, pid) => (t.players[pid] ? t.players[pid].s : 1e9);
function seedOrder(S) { let a = [1]; while (a.length < S) a = a.flatMap(x => [x, a.length * 2 + 1 - x]); return a; }
function tResolve(t, m, final) {
  if (m.w) return;
  const A = m.a, B = m.b, ra = m.rep && m.rep[A], rb = m.rep && m.rep[B];
  if (ra && rb) { if (ra === rb) { m.w = ra; m.how = 'agreed'; } else if (final) { m.w = seedOf(t, A) < seedOf(t, B) ? A : B; m.how = 'dispute'; } return; }
  const only = ra || rb, reporter = ra ? A : B;
  if (only && only !== reporter) { m.w = only; m.how = 'conceded'; return; }
  if (final) { if (only) { m.w = only; m.how = 'claimed'; } else { m.w = seedOf(t, A) < seedOf(t, B) ? A : B; m.how = 'no-show'; } }
}
function tPropagate(t) {
  const R = t.bracket.rounds;
  for (let r = 1; r < R.length; r++) R[r].forEach((m, i) => {
    const p1 = R[r - 1][2 * i], p2 = R[r - 1][2 * i + 1];
    if (!m.a && p1.w) m.a = p1.w; if (!m.b && p2.w) m.b = p2.w;
  });
}
function tBuild(t) {
  const ids = Object.keys(t.players).sort((x, y) => t.players[x].s - t.players[y].s);
  if (ids.length < TC.min) { t.status = 'cancelled'; return; }
  let S = 2; while (S < ids.length) S *= 2;
  const order = seedOrder(S), rounds = [];
  const r0 = []; for (let i = 0; i < S; i += 2) {
    const a = ids[order[i] - 1] || null, b = ids[order[i + 1] - 1] || null, m = { a, b, w: null, rep: {} };
    if (a && !b) { m.w = a; m.how = 'bye'; } else if (b && !a) { m.w = b; m.how = 'bye'; } r0.push(m);
  }
  rounds.push(r0);
  for (let n = S / 2; n >= 2; n /= 2) { const r = []; for (let i = 0; i < n / 2; i++) r.push({ a: null, b: null, w: null, rep: {} }); rounds.push(r); }
  t.bracket = { size: S, rounds }; t.status = 'running'; tPropagate(t);
}
function tAdvance(t, now) {
  let ch = false;
  if (t.status === 'reg' && now >= t.start + TC.regMs) {
    if (Object.keys(t.players).length >= TC.min) tBuild(t);
    else t.start += (Math.floor((now - (t.start + TC.regMs)) / TC.regMs) + 1) * TC.regMs;
    ch = true;
  }
  if (t.status === 'running') {
    const R = t.bracket.rounds;
    for (let r = 0; r < R.length; r++) {
      if (now >= re(t, r)) { R[r].forEach(m => { if (!m.w && m.a && m.b) { tResolve(t, m, true); ch = true; } }); tPropagate(t); }
    }
    const last = R[R.length - 1][0];
    if (last.w) { t.status = 'done'; t.champion = last.w; ch = true; }
  }
  return ch;
}
function tPlace(t, pid) {
  const p = t.players[pid]; if (!p) return '';
  if (t.status !== 'done') return '';
  const R = t.bracket.rounds, fin = R[R.length - 1][0];
  if (fin.w === pid) return 'champion';
  if (fin.a === pid || fin.b === pid) return 'finalist';
  if (R.length >= 2 && R[R.length - 2].some(m => (m.a === pid || m.b === pid))) return 'semi';
  return 'entered';
}
function tView(t, now, pid) {
  const names = {}; Object.keys(t.players).forEach(k => { names[k] = { n: t.players[k].n, f: t.players[k].f, s: t.players[k].s }; });
  const v = { id: t.id, start: t.start, now, status: t.status, regEnd: t.start + TC.regMs, roundMs: TC.roundMs, weekEnd: (t.wk || t.start) + TC.weekMs, min: TC.min, max: TC.max, count: Object.keys(t.players).length, players: t.status === 'reg' ? undefined : names, champion: t.champion || null };
  if (t.bracket) {
    v.rounds = t.bracket.rounds.map((R, r) => ({ rs: rs(t, r), re: re(t, r), m: R.map(m => ({ a: m.a, b: m.b, w: m.w, how: m.how || '' })) }));
    v.cur = Math.max(0, Math.min(t.bracket.rounds.length - 1, Math.floor((now - (t.start + TC.regMs)) / TC.roundMs)));
  }
  if (pid) {
    const me = { pid, entered: !!t.players[pid], place: tPlace(t, pid) };
    me.reward = me.place ? REWARD[me.place] : 0; me.claimed = !!t.claimed[pid];
    if (t.bracket && me.entered) {
      let r = t.bracket.rounds.findIndex((R, i) => R.some(m => (m.a === pid || m.b === pid) && !m.w));
      if (r < 0) r = -1;
      if (r >= 0) {
        const m = t.bracket.rounds[r].findIndex(x => (x.a === pid || x.b === pid) && !x.w), M = t.bracket.rounds[r][m], opp = M.a === pid ? M.b : M.a;
        me.match = { r, m, opp: opp ? { pid: opp, n: t.players[opp].n, f: t.players[opp].f } : null, open: !!opp && now >= rs(t, r) && now < re(t, r), reported: !!(M.rep && M.rep[pid]), rs: rs(t, r), re: re(t, r) };
      }
      me.out = !me.match && t.status !== 'done' ? true : false;
    }
    v.me = me;
  }
  return v;
}
async function tPrev(t, pid) {
  const old = t.prevId ? await Store.get('tour:' + t.prevId) : null;
  if (!old || !old.players || !old.players[pid]) return null;
  const place = tPlace(old, pid);
  return { id: old.id, place, reward: place ? REWARD[place] : 0, claimed: !!old.claimed[pid] };
}
let tLockP = Promise.resolve();
const tLock = fn => { const r = tLockP.then(fn, fn); tLockP = r.catch(() => {}); return r; };
async function tourHandle(url, body, ip, res, req) {
  if (!hit('t:' + ip, 90, 60000)) return sendJson(res, 429, { error: 'too many requests' });
  return tLock(async () => {
    const now = tnow(req), t = await tLoad(now);
    if (tAdvance(t, now)) await tSave(t);
    const tok = body && body.tok, pid = tokOk(tok) ? pidOf(tok) : '';
    if (url === '/api/tour/state') { const v = tView(t, now, pid); if (pid) { const pv = await tPrev(t, pid); if (pv) v.prev = pv; } return sendJson(res, 200, v); }
    if (!pid) return sendJson(res, 400, { error: 'bad token' });
    if (url === '/api/tour/join') {
      if (t.status !== 'reg') return sendJson(res, 409, { error: 'registration closed' });
      if (!t.players[pid]) {
        if (Object.keys(t.players).length >= TC.max) return sendJson(res, 409, { error: 'full' });
        const n = String(body.name || 'Fighter').replace(/[^\w .-]/g, '').slice(0, 10) || 'Fighter', f = /^[A-HJ-NP-Z2-9]{8}$/.test(String(body.fid || '')) ? body.fid : '';
        t.players[pid] = { n, f, s: ++t.n }; await tSave(t);
      }
      return sendJson(res, 200, tView(t, now, pid));
    }
    if (url === '/api/tour/report') {
      const r = body.r | 0, m = body.m | 0, M = t.bracket && t.bracket.rounds[r] && t.bracket.rounds[r][m];
      if (!M || !M.a || !M.b) return sendJson(res, 404, { error: 'no such match' });
      if (pid !== M.a && pid !== M.b) return sendJson(res, 403, { error: 'not your match' });
      if (M.w) return sendJson(res, 200, tView(t, now, pid));
      if (now < rs(t, r) || now >= re(t, r)) return sendJson(res, 409, { error: 'outside the round window' });
      M.rep = M.rep || {}; M.rep[pid] = body.win ? pid : (pid === M.a ? M.b : M.a);
      tResolve(t, M, false); tPropagate(t); if (t.bracket.rounds[t.bracket.rounds.length - 1][0].w) { t.status = 'done'; t.champion = t.bracket.rounds[t.bracket.rounds.length - 1][0].w; }
      await tSave(t); return sendJson(res, 200, tView(t, now, pid));
    }
    if (url === '/api/tour/claim') {
      let tt = t; if (body.tid && /^t\d{6,15}$/.test(String(body.tid)) && body.tid !== t.id) { tt = await Store.get('tour:' + body.tid); if (!tt) return sendJson(res, 404, { error: 'unknown tournament' }); }
      const place = tPlace(tt, pid);
      if (!place) return sendJson(res, 409, { error: 'nothing to claim' });
      if (tt.claimed[pid]) return sendJson(res, 409, { error: 'already claimed' });
      tt.claimed[pid] = 1; await tSave(tt); return sendJson(res, 200, { coins: REWARD[place], place });
    }
    return sendJson(res, 404, { error: 'unknown' });
  });
}

const validCode = c => typeof c === 'string' && /^\d{4}$/.test(c);
async function api(req, res, url) {
  const ip = clientIp(req);
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, GET, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Max-Age': '600' }); return res.end(); }
  if (url === '/api/ping' && req.method === 'GET') return sendJson(res, 200, { ok: true, persistent: Store.persistent });
  if (url === '/api/tour' && req.method === 'GET') { try { return await tLock(async () => { const now = tnow(req), t = await tLoad(now); if (tAdvance(t, now)) await tSave(t); return sendJson(res, 200, tView(t, now, '')); }); } catch (e) { return sendJson(res, 500, { error: 'server error' }); } }
  if (req.method !== 'POST') return sendJson(res, 405, { error: 'method' });
  let body; try { body = await readBody(req); } catch (e) { return sendJson(res, e.status || 400, { error: 'bad request' }); }
  try {
    if (url.startsWith('/api/tour/')) return await tourHandle(url, body, ip, res, req);
    if (url === '/api/load') {
      if (count('f:' + ip, 3600000) >= FAIL_LIMIT || !hit('l1:' + ip, 12, 60000) || !hit('l2:' + ip, 80, 3600000)) return sendJson(res, 429, { error: 'too many tries' });
      if (!validCode(body.code)) { hit('f:' + ip, 99, 3600000); return sendJson(res, 400, { error: 'bad code' }); }
      const rec = await Store.get(body.code);
      if (!rec) { hit('f:' + ip, 99, 3600000); return sendJson(res, 404, { error: 'not found' }); }
      return sendJson(res, 200, { data: rec.d });
    }
    if (url === '/api/save') {
      const data = cleanSave(body.data);
      if (!data) return sendJson(res, 400, { error: 'bad data' });
      if (body.code !== undefined && body.code !== null && body.code !== '') {
        if (count('f:' + ip, 3600000) >= FAIL_LIMIT || !hit('s:' + ip, 30, 60000)) return sendJson(res, 429, { error: 'too many tries' });
        if (!validCode(body.code)) return sendJson(res, 400, { error: 'bad code' });
        const rec = await Store.get(body.code);
        if (!rec) { hit('f:' + ip, 99, 3600000); return sendJson(res, 404, { error: 'not found' }); }
        await Store.put(body.code, { d: data, t: Date.now() });
        return sendJson(res, 200, { code: body.code });
      }
      if (!hit('c:' + ip, 10, 3600000)) return sendJson(res, 429, { error: 'too many new codes' });
      for (let i = 0; i < 80; i++) {
        const code = String(Math.floor(Math.random() * 10000)).padStart(4, '0');
        if (await Store.create(code, { d: data, t: Date.now() })) return sendJson(res, 200, { code });
      }
      return sendJson(res, 503, { error: 'full' });
    }
  } catch (e) { return sendJson(res, 500, { error: 'server error' }); }
  return sendJson(res, 404, { error: 'unknown' });
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url.startsWith('/api/')) return api(req, res, url);
  if (url === '/health') { res.writeHead(200); return res.end('ok'); }
  if (url === '/' || url === '/index.html') {
    fs.readFile(INDEX, (err, buf) => {
      if (err) { res.writeHead(500); return res.end('index.html missing'); }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(buf);
    });
    return;
  }
  res.writeHead(404); res.end('Not found');
});

const wss = new WebSocketServer({ server, maxPayload: MAX_MSG });
const rooms = new Map(); // roomName -> Map(peerId -> ws)
const validId = s => typeof s === 'string' && /^[a-z0-9]{8,32}$/.test(s);
const validRoom = s => typeof s === 'string' && s.length > 0 && s.length <= 64;
const send = (ws, o) => { if (ws.readyState === 1) ws.send(JSON.stringify(o)); };

function broadcast(room, o, except) {
  const r = rooms.get(room); if (!r) return;
  const s = JSON.stringify(o);
  r.forEach((p, id) => { if (id !== except && p.ws.readyState === 1) p.ws.send(s); });
}
function leave(ws, room) {
  const r = rooms.get(room);
  if (!r || !ws.rooms.has(room)) return;
  ws.rooms.delete(room);
  if (r.get(ws.id) && r.get(ws.id).ws === ws) {
    r.delete(ws.id);
    broadcast(room, { t: 'gone', room, peer: ws.id });
  }
  if (!r.size) rooms.delete(room);
}

wss.on('connection', ws => {
  ws.id = null; ws.rooms = new Set(); ws.alive = true; ws.count = 0;
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', data => {
    if (++ws.count > MAX_MSGS_PER_SEC) return ws.close(1008, 'rate');
    let m; try { m = JSON.parse(data.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'hello') {
      if (ws.id || !validId(m.id)) return;
      ws.id = m.id; return;
    }
    if (!ws.id) return;

    if (m.t === 'join' && validRoom(m.room)) {
      if (ws.rooms.has(m.room) || ws.rooms.size >= MAX_ROOMS_PER_SOCKET) return;
      let r = rooms.get(m.room); if (!r) rooms.set(m.room, r = new Map());
      if (r.size >= MAX_PEERS_PER_ROOM) return;
      const old = r.get(ws.id);
      if (old && old.ws !== ws) { // same id reconnecting: replace stale socket
        old.ws.rooms.delete(m.room);
      }
      r.set(ws.id, { ws, presence: {} });
      ws.rooms.add(m.room);
      const list = [];
      r.forEach((p, id) => list.push({ peer: id, presence: p.presence }));
      send(ws, { t: 'snap', room: m.room, list });
    } else if (m.t === 'pres' && validRoom(m.room) && ws.rooms.has(m.room)) {
      const p = m.p;
      if (!p || typeof p !== 'object' || Array.isArray(p)) return;
      const r = rooms.get(m.room); const me = r && r.get(ws.id);
      if (!me || me.ws !== ws) return;
      me.presence = p;
      broadcast(m.room, { t: 'upd', room: m.room, peer: ws.id, presence: p }, ws.id);
    } else if (m.t === 'leave' && validRoom(m.room)) {
      leave(ws, m.room);
    }
  });
  ws.on('close', () => { [...ws.rooms].forEach(r => leave(ws, r)); });
  ws.on('error', () => {});
});

const hb = setInterval(() => {
  wss.clients.forEach(ws => { if (!ws.alive) return ws.terminate(); ws.alive = false; ws.ping(); });
}, 30000);
const rl = setInterval(() => wss.clients.forEach(ws => { ws.count = 0; }), 1000);
wss.on('close', () => { clearInterval(hb); clearInterval(rl); });

server.listen(PORT, () => console.log('Night Market Brawl running on port ' + PORT));
