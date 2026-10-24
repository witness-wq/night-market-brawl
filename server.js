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
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
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
