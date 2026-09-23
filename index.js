const express = require('express');
const { WebSocketServer } = require('ws');
const Database = require('better-sqlite3');
const http = require('http');
const path = require('path');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const db = new Database('./data.db');
db.pragma('journal_mode = WAL');

db.exec(`CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY, name TEXT, role TEXT, pin TEXT, paired_with TEXT, created_at TEXT
)`);
db.exec(`CREATE TABLE IF NOT EXISTS kv (
  uid TEXT, key TEXT, value TEXT, updated_at TEXT, PRIMARY KEY(uid, key)
)`);

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

const clients = new Map(); // uid -> ws

// --- Auth ---
app.post('/api/register', (req, res) => {
  const { id, name, role, pin } = req.body;
  db.prepare(`INSERT OR REPLACE INTO users (id,name,role,pin,created_at) VALUES(?,?,?,?,?)`)
    .run(id, name, role, pin, new Date().toISOString());
  res.json({ id, name, role });
});

app.post('/api/login', (req, res) => {
  const { id, pin } = req.body;
  const row = db.prepare(`SELECT name,role,pin,paired_with FROM users WHERE id=?`).get(id);
  if (!row || row.pin !== pin) return res.status(401).json({ error: 'wrong' });
  res.json({ id, name: row.name, role: row.role, pairedWith: row.paired_with });
});

app.post('/api/bind', (req, res) => {
  const { myId, partnerId } = req.body;
  db.prepare(`UPDATE users SET paired_with=? WHERE id=?`).run(partnerId, myId);
  res.json({ ok: 1 });
});

// --- Data sync ---
app.post('/api/sync', (req, res) => {
  const { uid, data } = req.body;
  const insert = db.prepare(`INSERT OR REPLACE INTO kv (uid,key,value,updated_at) VALUES(?,?,?,?)`);
  const tx = db.transaction(() => {
    for (const [k, v] of Object.entries(data)) {
      insert.run(uid, k, v, new Date().toISOString());
    }
  });
  tx();
  // push patches to partner
  const me = db.prepare(`SELECT paired_with FROM users WHERE id=?`).get(uid);
  if (me && me.paired_with) {
    const partnerWs = clients.get(me.paired_with);
    if (partnerWs && partnerWs.readyState === 1) {
      for (const [k, v] of Object.entries(data)) {
        partnerWs.send(JSON.stringify({ type: 'patch', key: k, value: v }));
      }
    }
  }
  res.json({ ok: 1 });
});

app.get('/api/pull', (req, res) => {
  const { uid } = req.query;
  const rows = db.prepare(`SELECT key, value FROM kv WHERE uid=?`).all(uid);
  const data = {};
  rows.forEach(r => data[r.key] = r.value);
  res.json(data);
});

// --- WebSocket ---
wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const uid = url.searchParams.get('uid');
  if (!uid) return ws.close();
  clients.set(uid, ws);

  ws.on('message', (raw) => {
    try {
      const msg = JSON.parse(raw);
      if (msg.type === 'chat' || msg.type === 'patch') {
        const me = db.prepare(`SELECT paired_with FROM users WHERE id=?`).get(uid);
        if (me && me.paired_with) {
          const partnerWs = clients.get(me.paired_with);
          if (partnerWs && partnerWs.readyState === 1) {
            partnerWs.send(JSON.stringify(msg));
          }
        }
      }
    } catch (e) {}
  });

  ws.on('close', () => clients.delete(uid));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log('Server on ' + PORT));
//（注：内容由AI生成）
