const express = require('express');
const cors = require('cors');
const path = require('path');
const fs = require('fs');
require('dotenv').config();

const admin = require('firebase-admin');

const serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL
});

const db = admin.database();
const app = express();

app.use(express.json());
app.use(cors());

// Auth: all /api routes require the token (SSE uses query param, rest use header)
app.use('/api', (req, res, next) => {
  const token = req.query.token || req.headers['x-app-token'];
  if (token !== process.env.APP_TOKEN)
    return res.status(403).json({ error: 'Forbidden' });
  next();
});

// Serve index.html with APP_TOKEN injected so it never appears in source files
app.get('/', (req, res) => {
  const html = fs.readFileSync(path.join(__dirname, 'public', 'index.html'), 'utf8');
  res.send(html.replace('__APP_TOKEN__', process.env.APP_TOKEN || ''));
});

app.use(express.static(path.join(__dirname, 'public')));

// SSE – streams real-time updates for multiple DB paths over one connection
app.get('/api/stream', (req, res) => {
  const pathsParam = req.query.paths;
  if (!pathsParam) return res.status(400).json({ error: 'paths required' });
  const paths = pathsParam.split(',').filter(Boolean);

  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('X-Accel-Buffering', 'no');
  res.flushHeaders();

  const keepAlive = setInterval(() => res.write(':ping\n\n'), 20000);

  const listeners = paths.map(p => {
    const ref = db.ref(p);
    const listener = ref.on(
      'value',
      snapshot => res.write(`data: ${JSON.stringify({ path: p, data: snapshot.val() })}\n\n`),
      err => console.error('Firebase listener error on', p, err)
    );
    return { ref, listener };
  });

  req.on('close', () => {
    clearInterval(keepAlive);
    listeners.forEach(({ ref, listener }) => ref.off('value', listener));
  });
});

// Set (overwrite)
app.post('/api/write', async (req, res) => {
  const { path, value } = req.body;
  if (!path) return res.status(400).json({ error: 'path required' });
  try {
    await db.ref(path).set(value);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Update (merge)
app.post('/api/patch', async (req, res) => {
  const { path, value } = req.body;
  if (!path || typeof value !== 'object' || value === null)
    return res.status(400).json({ error: 'path and value object required' });
  try {
    await db.ref(path).update(value);
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Push (append with auto-key)
app.post('/api/push', async (req, res) => {
  const { path, value } = req.body;
  if (!path) return res.status(400).json({ error: 'path required' });
  try {
    const ref = await db.ref(path).push(value);
    res.json({ ok: true, key: ref.key });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Delete
app.delete('/api/data', async (req, res) => {
  const p = req.query.path;
  if (!p) return res.status(400).json({ error: 'path required' });
  try {
    await db.ref(p).remove();
    res.json({ ok: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
