require('dotenv').config();
const express = require('express');
const http = require('http');
const crypto = require('crypto');
const QRCode = require('qrcode');
const WebSocket = require('ws');
const mongoose = require('mongoose');
const FonepayService = require('./fonepayService');
const Transaction = require('./models/Transaction');
const { createCanvas, loadImage } = require('canvas');

const app = express();
app.use(express.json());
app.use(express.static('public'));

const server = http.createServer(app);

const QR_CANVAS_SIZE = 320;
const LOGO_SIZE = 90;
const LOGO_PADDING = 4;

const MONGODB_URI_FONEPAY = process.env.MONGODB_URI_FONEPAY || 'mongodb://127.0.0.1:27017/fonepay_qr';
mongoose
  .connect(MONGODB_URI_FONEPAY)
  .then(() => console.log('Connected to MongoDB'))
  .catch((err) => {
    console.error('MongoDB connection failed:', err.message);
  });

const fonepay = new FonepayService({
  merchantCode: process.env.FONEPAY_MERCHANT_CODE,
  secretKey: process.env.FONEPAY_SECRET_KEY,
  username: process.env.FONEPAY_USERNAME,
  password: process.env.FONEPAY_PASSWORD,
  baseUrl: process.env.FONEPAY_BASE_URL || 'https://merchantapi.fonepay.com/api',
});

const wss = new WebSocket.Server({ server, path: '/ws' });

const orders = new Map();
const PAYMENT_TIMEOUT_MS = 15 * 60 * 1000;

function broadcast(prn, payload) {
  const order = orders.get(prn);
  if (!order) return;
  const msg = JSON.stringify(payload);
  for (const clientWs of order.clientSockets) {
    if (clientWs.readyState === WebSocket.OPEN) clientWs.send(msg);
  }
}

function saveStatus(prn, update) {
  Transaction.updateOne({ prn }, update).catch((err) =>
    console.error(`[db:${prn}] failed to update status`, err.message)
  );
}

function subscribeToFonepay(prn, fonepayWsUrl) {
  const order = orders.get(prn);
  if (!order) return;

  const socket = new WebSocket(fonepayWsUrl);
  order.fonepaySocket = socket;

  socket.on('message', async (raw) => {
    const rawText = raw.toString();
    // console.log(`[fonepay:${prn}] raw ws message:`, rawText);

    try {
      const outer = JSON.parse(rawText);
      const status = JSON.parse(outer.transactionStatus);

      if (status.qrVerified) {
        order.status = 'qr_scanned';
        broadcast(prn, { type: 'qr_scanned' });
        saveStatus(prn, { status: 'qr_scanned' });
        return;
      }

      if (status.cancelled) {
        order.status = 'failed';
        broadcast(prn, { type: 'failed', message: status.message || 'Payment cancelled' });
        saveStatus(prn, { status: 'failed' });
        cleanupOrder(prn);
        return;
      }

      if (typeof status.paymentSuccess !== 'undefined') {
        const confirmed = await fonepay.checkStatus(prn);

        if (confirmed.paymentStatus === 'success') {
          order.status = 'paid';
          broadcast(prn, {
            type: 'paid',
            amount: status.amount,
            traceId: confirmed.fonepayTraceId,
          });
          saveStatus(prn, { status: 'paid', fonepayTraceId: confirmed.fonepayTraceId });
        } else {
          order.status = 'failed';
          broadcast(prn, { type: 'failed' });
          saveStatus(prn, { status: 'failed' });
        }

        cleanupOrder(prn);
      }
    } catch (err) {
      console.error(`[fonepay:${prn}] failed to parse ws message`, err);
    }
  });

  socket.on('error', (err) => {
    console.error(`[fonepay:${prn}] websocket error`, err.message);
    broadcast(prn, { type: 'error', message: 'Connection to Fonepay lost' });
  });

  order.timeoutHandle = setTimeout(() => {
    if (order.status === 'pending' || order.status === 'qr_scanned') {
      order.status = 'timeout';
      broadcast(prn, { type: 'timeout' });
      saveStatus(prn, { status: 'timeout' });
      cleanupOrder(prn);
    }
  }, PAYMENT_TIMEOUT_MS);
}

function cleanupOrder(prn) {
  const order = orders.get(prn);
  if (!order) return;
  clearTimeout(order.timeoutHandle);
  if (order.fonepaySocket) order.fonepaySocket.close();
  for (const clientWs of order.clientSockets) clientWs.close();
  setTimeout(() => orders.delete(prn), 5 * 60 * 1000);
}

function generatePrn() {
  return crypto.randomBytes(9).toString('hex');
}

function generateInvoiceId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let result = '';
  for (let i = 0; i < 6; i++) {
    result += chars.charAt(crypto.randomInt(0, chars.length));
  }
  return result;
}

const qrRequestLog = new Map();
const QR_RATE_LIMIT_WINDOW_MS = 60 * 1000;
const QR_RATE_LIMIT_MAX = 10;

function qrRateLimiter(req, res, next) {
  const ip = req.ip;
  const now = Date.now();
  const timestamps = (qrRequestLog.get(ip) || []).filter(
    (t) => now - t < QR_RATE_LIMIT_WINDOW_MS
  );

  if (timestamps.length >= QR_RATE_LIMIT_MAX) {
    return res.status(429).json({ error: 'Too many QR requests — please wait a moment and try again.' });
  }

  timestamps.push(now);
  qrRequestLog.set(ip, timestamps);
  next();
}

function requireAdminKey(req, res, next) {
  const expected = process.env.ADMIN_API_KEY;
  if (!expected) {
    console.error('ADMIN_API_KEY is not set — refusing admin request. Set it in .env.');
    return res.status(500).json({ error: 'Server misconfigured' });
  }
  const provided = req.get('x-api-key');
  if (provided !== expected) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  next();
}

const ALLOWED_STATUSES = ['pending', 'qr_scanned', 'paid', 'failed', 'timeout'];

app.post('/api/qr', qrRateLimiter, async (req, res) => {
  try {
    const { amount, remarks1 } = req.body;
    const numAmount = Number(amount);
    if (!amount || Number.isNaN(numAmount) || numAmount <= 0) {
      return res.status(400).json({ error: 'A positive amount is required' });
    }

    const prn = generatePrn();
    const finalRemarks1 = String(remarks1 || 'Order').replace(/,/g, ' ').trim().slice(0, 25);
    const invoiceNo = generateInvoiceId();
    const finalRemarks2 = `INV-${invoiceNo}`;

    const fonepayRes = await fonepay.generateQr({
      amount,
      prn,
      remarks1: finalRemarks1,
      remarks2: finalRemarks2,
    });

    const orderToken = crypto.randomUUID();
    orders.set(prn, {
      amount,
      status: 'pending',
      token: orderToken,
      clientSockets: new Set(),
    });

    subscribeToFonepay(prn, fonepayRes.thirdpartyQrWebSocketUrl);

    Transaction.create({
      prn,
      amount: Number(amount),
      remarks1: finalRemarks1,
      remarks2: finalRemarks2,
      invoiceNo,
      status: 'pending',
      qrMessage: fonepayRes.qrMessage,
    }).catch((err) => console.error(`[db:${prn}] failed to save transaction`, err.message));

    const canvas = createCanvas(QR_CANVAS_SIZE, QR_CANVAS_SIZE);
    await QRCode.toCanvas(canvas, fonepayRes.qrMessage, {
      width: QR_CANVAS_SIZE,
      margin: 1,
      errorCorrectionLevel: 'H',
    });

    const ctx = canvas.getContext('2d');

    try {
      const logoPath = 'images/logo.png';
      const logo = await loadImage(logoPath);

      const centerX = QR_CANVAS_SIZE / 2;
      const centerY = QR_CANVAS_SIZE / 2;
      const halfLogo = LOGO_SIZE / 2;
      const radius = halfLogo + LOGO_PADDING;

      ctx.beginPath();
      ctx.arc(centerX, centerY, radius, 0, Math.PI * 2);
      ctx.fillStyle = '#ffffff';
      ctx.fill();

      ctx.drawImage(logo, centerX - halfLogo, centerY - halfLogo, LOGO_SIZE, LOGO_SIZE);
    } catch (logoErr) {
      console.warn('Could not load logo, generating standard QR without center logo:', logoErr.message);
    }

    const qrImageDataUrl = canvas.toDataURL();

    res.json({
      prn,
      orderToken,
      invoiceNo: finalRemarks2,
      qrImageDataUrl,
      expiresInSeconds: PAYMENT_TIMEOUT_MS / 1000,
    });
  } catch (err) {
    console.error('QR generation failed:', err.details || err.message);
    res.status(502).json({ error: 'Could not generate QR', details: err.details });
  }
});

app.get('/api/status/:prn', async (req, res) => {
  const order = orders.get(req.params.prn);
  if (!order || order.token !== req.get('x-order-token')) {
    return res.status(404).json({ error: 'Unknown or expired order' });
  }
  try {
    const result = await fonepay.checkStatus(req.params.prn);
    res.json(result);
  } catch (err) {
    res.status(502).json({ error: 'Could not fetch status' });
  }
});

app.get('/api/transactions', requireAdminKey, async (req, res) => {
  try {
    const page = Math.max(parseInt(req.query.page, 10) || 1, 1);
    const limit = Math.min(parseInt(req.query.limit, 10) || 25, 100);
    const filter = {};
    if (typeof req.query.status === 'string' && ALLOWED_STATUSES.includes(req.query.status)) {
      filter.status = req.query.status;
    }

    const [transactions, total] = await Promise.all([
      Transaction.find(filter)
        .sort({ createdAt: -1 })
        .skip((page - 1) * limit)
        .limit(limit)
        .lean(),
      Transaction.countDocuments(filter),
    ]);

    res.json({ transactions, total, page, limit, pages: Math.ceil(total / limit) });
  } catch (err) {
    console.error('Failed to fetch transactions:', err.message);
    res.status(500).json({ error: 'Could not fetch transactions' });
  }
});

app.get('/api/transactions/:prn', requireAdminKey, async (req, res) => {
  try {
    const transaction = await Transaction.findOne({ prn: req.params.prn }).lean();
    if (!transaction) return res.status(404).json({ error: 'Transaction not found' });
    res.json(transaction);
  } catch (err) {
    res.status(500).json({ error: 'Could not fetch transaction' });
  }
});

wss.on('connection', (clientWs, req) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const prn = url.searchParams.get('prn');
  const token = url.searchParams.get('token');
  const order = prn && orders.get(prn);

  if (!order || order.token !== token) {
    clientWs.send(JSON.stringify({ type: 'error', message: 'Unknown or expired order' }));
    clientWs.close();
    return;
  }

  order.clientSockets.add(clientWs);
  clientWs.send(JSON.stringify({ type: 'status', status: order.status }));

  clientWs.on('close', () => order.clientSockets.delete(clientWs));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`Fonepay QR demo running at http://localhost:${PORT}`);
});