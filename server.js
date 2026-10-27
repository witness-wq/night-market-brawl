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
const validCode = c => typeof c === 'string' && /^\d{4}$/.test(c);
async function api(req, res, url) {
  const ip = clientIp(req);
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, GET, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Max-Age': '600' }); return res.end(); }
  if (url === '/api/ping' && req.method === 'GET') return sendJson(res, 200, { ok: true, persistent: Store.persistent });
  if (req.method !== 'POST') return sendJson(res, 405, { error: 'method' });
  let body; try { body = await readBody(req); } catch (e) { return sendJson(res, e.status || 400, { error: 'bad request' }); }
  try {
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
