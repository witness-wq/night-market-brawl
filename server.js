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
      if (banFor(String(body.fid || ''), '')) return sendJson(res, 403, { error: 'banned' });
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


/* ---------- moderation (enforced by the server) ---------- */
// Set ADMIN_KEY (full access) and optionally MOD_KEY (kick, mute, rename) as environment variables on your host.
// Keys need at least 8 characters. Without ADMIN_KEY the admin tools are switched off.
const ACR = require('crypto');
const ADM_KEYS = [['admin', process.env.ADMIN_KEY], ['mod', process.env.MOD_KEY]].filter(([, k]) => k && String(k).length >= 8);
const SHA = s => ACR.createHash('sha256').update(String(s)).digest();
function roleOf(key) { if (typeof key !== 'string' || !key) return ''; const h = SHA(key); for (const [r, k] of ADM_KEYS) { if (ACR.timingSafeEqual(h, SHA(k))) return r; } return ''; }
const AST = { bans: [], mutes: {}, names: {}, words: ['admin', 'moderator', 'staff', 'owner', 'official'], log: [], lastGm: null };
(async () => { try { const d = await Store.get('admin:state'); if (d && typeof d === 'object') { for (const k of Object.keys(AST)) if (d[k] !== undefined) AST[k] = d[k]; } } catch {} })();
let aSaveT = null; const aSave = () => { clearTimeout(aSaveT); aSaveT = setTimeout(() => { Store.put('admin:state', AST).catch(() => {}); }, 400); };
const aLog = (role, act, target, detail) => { AST.log.push({ t: Date.now(), role, act, target: String(target || '').slice(0, 40), detail: String(detail || '').slice(0, 120) }); if (AST.log.length > 200) AST.log.splice(0, AST.log.length - 200); aSave(); };
const LEET = { 0: 'o', 1: 'i', 3: 'e', 4: 'a', 5: 's', 7: 't', '@': 'a', '$': 's', '!': 'i' };
const normName = s => String(s).toLowerCase().replace(/[013457@$!]/g, c => LEET[c]).replace(/[^a-z]/g, '');
const nameBad = n => { const t = normName(n); return AST.words.some(w => { const x = normName(w); return x && t.includes(x); }); };
const fallbackName = (fid, peer) => 'Player' + (parseInt(SHA(fid || peer || 'x').toString('hex').slice(0, 6), 16) % 9000 + 1000);
const FIDRE = /^[A-HJ-NP-Z2-9]{8}$/;
const ipHash = ip => SHA('nmb-salt:' + ip).toString('hex').slice(0, 16);
const ipMask = ip => { ip = String(ip || ''); const m = ip.match(/^(\d+)\.(\d+)\.\d+\.\d+$/); if (m) return m[1] + '.' + m[2] + '.x.x'; const g = ip.split(':'); return g.length > 2 ? g[0] + ':' + g[1] + '::' : 'hidden'; };
const isMuted = ws => !!(ws.fid && AST.mutes[ws.fid] && AST.mutes[ws.fid] > Date.now());
function banFor(fid, ipH) { const now = Date.now(); return AST.bans.find(b => (!b.until || b.until > now) && ((fid && b.fid === fid) || (ipH && b.ipH && b.ipH === ipH))) || null; }
function shownName(ws, name) { const f = ws.fid && AST.names[ws.fid]; if (f) return f; if (typeof name === 'string' && nameBad(name)) return fallbackName(ws.fid, ws.id); return name; }
// presence goes through here before it is stored and passed on: names are filtered or forced, muted players lose quick chat
function cleanPres(ws, p) {
  if (typeof p.fid === 'string' && FIDRE.test(p.fid)) ws.fid = p.fid;
  const lkName = p.lk && typeof p.lk === 'object' ? p.lk.name : undefined;
  const fixFn = p.fn !== undefined ? shownName(ws, p.fn) : undefined, fixLk = lkName !== undefined ? shownName(ws, lkName) : undefined;
  const mute = isMuted(ws) && p.pk && typeof p.pk === 'object' && p.pk.qc;
  if (fixFn === p.fn && fixLk === lkName && !mute) { if (typeof p.fn === 'string') ws.shown = p.fn; else if (typeof lkName === 'string') ws.shown = lkName; return p; }
  const q = { ...p };
  if (fixFn !== undefined) q.fn = fixFn;
  if (q.lk && typeof q.lk === 'object' && fixLk !== undefined) q.lk = { ...q.lk, name: fixLk };
  if (mute) q.pk = { ...q.pk, qc: 0 };
  ws.shown = typeof q.fn === 'string' ? q.fn : (q.lk && q.lk.name) || ws.shown;
  return q;
}
function refreshPresence(ws) { ws.rooms.forEach(room => { const r = rooms.get(room), me = r && r.get(ws.id); if (me && me.ws === ws) { me.presence = cleanPres(ws, me.presence); broadcast(room, { t: 'upd', room, peer: ws.id, presence: me.presence }, ws.id); } }); }
function sendBan(ws, b) { send(ws, { t: 'ban', reason: b.reason || '', until: b.until || 0 }); setTimeout(() => { try { ws.close(4003, 'banned'); } catch {} }, 150); }
const everyone = o => { const s = JSON.stringify(o); wss.clients.forEach(c => { if (c.readyState === 1 && c.id) c.send(s); }); };
const playerStatus = ws => { const l = rooms.get('lobby'), me = l && l.get(ws.id), p = (me && me.presence) || {}; return p.bz ? 'in a match' : p.srv ? 'park: ' + p.srv : (p.seek || p.tq) ? 'searching' : 'online'; };
let peakOnline = 0; const startedAt = Date.now();
function adminList() {
  const out = []; wss.clients.forEach(ws => { if (!ws.id) return; out.push({ peer: ws.id, name: ws.shown || '(no name yet)', fid: ws.fid || '', ip: ipMask(ws.ip), since: ws.since, st: playerStatus(ws), muted: isMuted(ws), forced: !!(ws.fid && AST.names[ws.fid]), rooms: ws.rooms.size }); });
  return out.sort((a, b) => a.name.localeCompare(b.name));
}
const findWs = peer => { let f = null; wss.clients.forEach(ws => { if (ws.id === peer) f = ws; }); return f; };
const cleanReason = r => String(r || '').replace(/[\u0000-\u001f]/g, ' ').slice(0, 80);
const cleanNick = n => String(n || '').replace(/[^\w .-]/g, '').trim().slice(0, 10);
async function adminHandle(url, body, ip, res) {
  if (!ADM_KEYS.length) return sendJson(res, 501, { error: 'admin tools are switched off (no ADMIN_KEY set)' });
  if (count('af:' + ip, 600000) >= 8) return sendJson(res, 429, { error: 'too many tries' });
  const role = roleOf(body && body.key);
  if (!role) { hit('af:' + ip, 99, 600000); return sendJson(res, 403, { error: 'wrong key' }); }
  const full = role === 'admin', no = () => sendJson(res, 403, { error: 'admins only' }), act = url.slice('/api/admin/'.length);
  if (act === 'login') return sendJson(res, 200, { role });
  if (act === 'players') return sendJson(res, 200, { players: adminList(), now: Date.now() });
  if (act === 'stats') { let n = 0; wss.clients.forEach(c => { if (c.id) n++; }); return sendJson(res, 200, { online: n, peak: peakOnline, rooms: rooms.size, uptime: Math.round((Date.now() - startedAt) / 1000), memMB: Math.round(process.memoryUsage().rss / 1048576), persistent: Store.persistent, bans: AST.bans.filter(b => !b.until || b.until > Date.now()).length, role }); }
  if (act === 'log') return sendJson(res, 200, { log: AST.log.slice(-100).reverse() });
  if (act === 'bans') return sendJson(res, 200, { bans: AST.bans.filter(b => !b.until || b.until > Date.now()).map(b => ({ id: b.id, name: b.name, fid: b.fid, ip: !!b.ipH, ipM: b.ipM, reason: b.reason, until: b.until, by: b.by, at: b.at })) });
  const t = findWs(String(body.peer || ''));
  if (act === 'kick') { if (!t) return sendJson(res, 404, { error: 'player is gone' }); send(t, { t: 'kick' }); setTimeout(() => { try { t.close(4001, 'kicked'); } catch {} }, 150); aLog(role, 'kick', t.shown, ''); return sendJson(res, 200, { ok: 1 }); }
  if (act === 'mute' || act === 'unmute') {
    if (!t || !t.fid) return sendJson(res, 404, { error: 'player is gone or has no id yet' });
    if (act === 'mute') AST.mutes[t.fid] = Date.now() + Math.max(1, Math.min(1440, +body.minutes || 15)) * 60000; else delete AST.mutes[t.fid];
    aLog(role, act, t.shown, act === 'mute' ? (+body.minutes || 15) + ' min' : ''); refreshPresence(t); return sendJson(res, 200, { ok: 1 });
  }
  if (act === 'rename') {
    if (!t || !t.fid) return sendJson(res, 404, { error: 'player is gone or has no id yet' });
    const n = cleanNick(body.name); const old = t.shown;
    if (n) { AST.names[t.fid] = n; send(t, { t: 'ren', name: n }); } else delete AST.names[t.fid];
    aLog(role, n ? 'rename' : 'clear-rename', old, n); refreshPresence(t); return sendJson(res, 200, { ok: 1 });
  }
  if (!full) return no();
  if (act === 'ban') {
    if (!t) return sendJson(res, 404, { error: 'player is gone' });
    const mins = Math.max(0, Math.min(525600, +body.minutes || 0)), b = { id: ACR.randomBytes(5).toString('hex'), name: t.shown, fid: t.fid || '', ipH: body.ip ? ipHash(t.ip) : '', ipM: body.ip ? ipMask(t.ip) : '', reason: cleanReason(body.reason), until: mins ? Date.now() + mins * 60000 : 0, by: role, at: Date.now() };
    if (!b.fid && !b.ipH) b.ipH = ipHash(t.ip), b.ipM = ipMask(t.ip);
    AST.bans.push(b); aLog(role, 'ban', t.shown, (mins ? mins + ' min' : 'permanent') + (b.reason ? ' - ' + b.reason : '')); sendBan(t, b);
    wss.clients.forEach(c => { if (c !== t && c.id && ((b.fid && c.fid === b.fid) || (b.ipH && ipHash(c.ip) === b.ipH))) sendBan(c, b); });
    return sendJson(res, 200, { ok: 1 });
  }
  if (act === 'unban') { const i = AST.bans.findIndex(b => b.id === body.id); if (i < 0) return sendJson(res, 404, { error: 'no such ban' }); const b = AST.bans.splice(i, 1)[0]; aLog(role, 'unban', b.name, ''); return sendJson(res, 200, { ok: 1 }); }
  if (act === 'announce') {
    const text = String(body.text || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, 140); if (!text) return sendJson(res, 400, { error: 'empty message' });
    const look = body.look && typeof body.look === 'object' && JSON.stringify(body.look).length < 3000 ? body.look : null;
    const g = { t: 'gm', id: ACR.randomBytes(5).toString('hex'), name: cleanNick(body.name) || 'Admin', look, text, sec: Math.max(5, Math.min(30, +body.sec || 10)), ts: Date.now() };
    AST.lastGm = g; let n = 0; wss.clients.forEach(c => { if (c.id) n++; }); everyone(g); aLog(role, 'announce', g.name, text); return sendJson(res, 200, { ok: 1, sentTo: n });
  }
  if (act === 'words-get') return sendJson(res, 200, { words: AST.words });
  if (act === 'words-set') { AST.words = (Array.isArray(body.words) ? body.words : []).map(w => String(w).trim().slice(0, 30)).filter(Boolean).slice(0, 300); aLog(role, 'name-filter', AST.words.length + ' words', ''); wss.clients.forEach(c => { if (c.id) refreshPresence(c); }); return sendJson(res, 200, { ok: 1, words: AST.words }); }
  return sendJson(res, 404, { error: 'unknown' });
}
setInterval(() => { const now = Date.now(); const n = AST.bans.length; AST.bans = AST.bans.filter(b => !b.until || b.until > now); for (const k of Object.keys(AST.mutes)) if (AST.mutes[k] <= now) delete AST.mutes[k]; if (AST.bans.length !== n) aSave(); }, 60000).unref();

const validCode = c => typeof c === 'string' && /^\d{4}$/.test(c);
async function api(req, res, url) {
  const ip = clientIp(req);
  if (req.method === 'OPTIONS') { res.writeHead(204, { 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Methods': 'POST, GET, OPTIONS', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Max-Age': '600' }); return res.end(); }
  if (url === '/api/ping' && req.method === 'GET') return sendJson(res, 200, { ok: true, persistent: Store.persistent });
  if (url === '/api/tour' && req.method === 'GET') { try { return await tLock(async () => { const now = tnow(req), t = await tLoad(now); if (tAdvance(t, now)) await tSave(t); return sendJson(res, 200, tView(t, now, '')); }); } catch (e) { return sendJson(res, 500, { error: 'server error' }); } }
  if (req.method !== 'POST') return sendJson(res, 405, { error: 'method' });
  let body; try { body = await readBody(req); } catch (e) { return sendJson(res, e.status || 400, { error: 'bad request' }); }
  try {
    if (url.startsWith('/api/admin/')) return await adminHandle(url, body, ip, res);
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

wss.on('connection', (ws, req) => {
  ws.id = null; ws.rooms = new Set(); ws.alive = true; ws.count = 0; ws.since = Date.now(); ws.fid = ''; ws.shown = '';
  { const xf = String(req.headers['x-forwarded-for'] || '').split(',').map(v => v.trim()).filter(Boolean); ws.ip = xf.length ? xf[xf.length - 1] : req.socket.remoteAddress || 'x'; }
  { const b = banFor('', ipHash(ws.ip)); if (b) { sendBan(ws, b); return; } }
  { const g = AST.lastGm; if (g && Date.now() - g.ts < 45000) send(ws, g); }
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', data => {
    if (++ws.count > MAX_MSGS_PER_SEC) return ws.close(1008, 'rate');
    let m; try { m = JSON.parse(data.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.t === 'hello') {
      if (ws.id || !validId(m.id)) return;
      ws.id = m.id; { let n = 0; wss.clients.forEach(c => { if (c.id) n++; }); if (n > peakOnline) peakOnline = n; } return;
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
      const q = cleanPres(ws, p);
      if (ws.fid) { const b = banFor(ws.fid, ''); if (b) { sendBan(ws, b); return; } }
      me.presence = q;
      broadcast(m.room, { t: 'upd', room: m.room, peer: ws.id, presence: q }, ws.id);
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
