require('dotenv').config();
const express = require('express');
const http = require('http');
const crypto = require('crypto');
const QRCode = require('qrcode');
const WebSocket = require('ws');
const { connectDatabase } = require('./connection');
const FonepayService = require('./fonepayService');
const Transaction = require('./models/Transaction');
const { createCanvas, loadImage } = require('@napi-rs/canvas');
const PDFDocument = require('pdfkit');

const app = express();
app.use(express.json());
app.use(express.static('public'));

connectDatabase();

app.get('/transactions', (req, res) => {
  res.sendFile(require('path').join(__dirname, 'public', 'transactions.html'));
});

const server = http.createServer(app);

const QR_CANVAS_SIZE = 320;
const LOGO_SIZE = 90;
const LOGO_PADDING = 4;

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
          const billToken = generateBillToken();
          broadcast(prn, {
            type: 'paid',
            amount: status.amount,
            traceId: confirmed.fonepayTraceId,
            billUrl: `/bill/${billToken}`,
          });
          saveStatus(prn, {
            status: 'paid',
            fonepayTraceId: confirmed.fonepayTraceId,
            billToken,
            paidAt: new Date(),
          });
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

function generateBillToken() {
  return crypto.randomBytes(16).toString('base64url');
}

function generateInvoiceId() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let result = '';
  for (let i = 0; i < 6; i++) {
    result += chars.charAt(crypto.randomInt(0, chars.length));
  }
  return result;
}

const MAX_REMARKS2_LENGTH = 25;

function pad2(n) {
  return String(n).padStart(2, '0');
}

function generateRemarksTimestamp() {
  const now = new Date();
  const yy = pad2(now.getFullYear() % 100);
  const mm = pad2(now.getMonth() + 1);
  const dd = pad2(now.getDate());
  const hh = pad2(now.getHours());
  const min = pad2(now.getMinutes());
  return `${yy}${mm}${dd}${hh}${min}`;
}

function sanitizeRemarks1(value) {
  return String(value || '')
    .replace(/[^a-zA-Z0-9 ]/g, '')
    .trim()
    .slice(0, 12);
}

function buildRemarks2({ invoiceNo }) {
  const timestamp = generateRemarksTimestamp();
  return `${timestamp}-${invoiceNo}`;
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
    const invoiceNo = generateInvoiceId();
    const finalRemarks1 = sanitizeRemarks1(remarks1) || 'Payment';
    const finalRemarks2 = buildRemarks2({ invoiceNo });

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
      invoiceNo: `INV-${invoiceNo}`,
      qrImageDataUrl,
      expiresInSeconds: PAYMENT_TIMEOUT_MS / 1000,
    });
  } catch (err) {
    console.error('QR generation failed:', err.details || err.message);
    res.status(502).json({ error: 'Could not generate QR', details: err.details });
  }
});

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function renderBillHtml(transaction) {
  const paidAt = transaction.paidAt || transaction.updatedAt || transaction.createdAt;
  return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1.0" />
<title>Receipt INV-${escapeHtml(transaction.invoiceNo)}</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    background: linear-gradient(135deg, #f1f5f9 0%, #e2e8f0 100%);
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 24px 16px;
    color: #0f172a;
  }
  .receipt {
    background: #ffffff;
    border-radius: 24px;
    padding: 36px 32px;
    max-width: 440px;
    width: 100%;
    box-shadow: 0 20px 40px -15px rgba(15, 23, 42, 0.12);
    border: 1px solid rgba(226, 232, 240, 0.8);
  }
  .header {
    text-align: center;
    margin-bottom: 24px;
  }
  .success-icon {
    width: 52px;
    height: 52px;
    background: #dcfce7;
    color: #15803d;
    border-radius: 50%;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    margin-bottom: 12px;
  }
  .success-icon svg {
    width: 28px;
    height: 28px;
    stroke-width: 2.5;
  }
  h1 {
    font-size: 1.35rem;
    font-weight: 700;
    color: #0f172a;
    margin-bottom: 6px;
  }
  .paid-badge {
    display: inline-block;
    background: #dcfce7;
    color: #15803d;
    font-weight: 700;
    font-size: 0.75rem;
    letter-spacing: 0.05em;
    padding: 4px 12px;
    border-radius: 9999px;
    text-transform: uppercase;
  }
  .hero-amount {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 16px;
    padding: 20px;
    text-align: center;
    margin-bottom: 24px;
  }
  .hero-amount .label {
    font-size: 0.75rem;
    font-weight: 600;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 4px;
  }
  .hero-amount .val {
    font-size: 1.85rem;
    font-weight: 800;
    color: #0f172a;
  }
  .details-list {
    margin-bottom: 28px;
  }
  .row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 12px 0;
    border-bottom: 1px solid #f1f5f9;
    font-size: 0.9rem;
  }
  .row:last-of-type {
    border-bottom: none;
  }
  .row .label {
    color: #64748b;
    font-weight: 500;
  }
  .row .val {
    color: #0f172a;
    font-weight: 600;
    text-align: right;
    word-break: break-all;
    margin-left: 16px;
  }
  .download {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    width: 100%;
    padding: 14px 20px;
    background: #0f172a;
    color: #ffffff;
    text-decoration: none;
    border-radius: 14px;
    font-weight: 600;
    font-size: 0.95rem;
    transition: all 0.2s ease;
    box-shadow: 0 4px 12px rgba(15, 23, 42, 0.15);
  }
  .download:hover {
    background: #1e293b;
    transform: translateY(-1px);
    box-shadow: 0 6px 16px rgba(15, 23, 42, 0.2);
  }
  .download svg {
    width: 18px;
    height: 18px;
  }
  .footer-note {
    margin-top: 20px;
    text-align: center;
    font-size: 0.75rem;
    color: #94a3b8;
  }
</style>
</head>
<body>
  <div class="receipt">
    <div class="header">
      <div class="success-icon">
        <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path stroke-linecap="round" stroke-linejoin="round" d="M4.5 12.75l6 6 9-13.5" />
        </svg>
      </div>
      <h1>Payment Receipt</h1>
      <span class="paid-badge">Paid</span>
    </div>

    <div class="hero-amount">
      <div class="label">Amount Paid</div>
      <div class="val">रु ${escapeHtml(transaction.amount)}</div>
    </div>

    <div class="details-list">
      <div class="row"><span class="label">Invoice No</span><span class="val">${escapeHtml(transaction.invoiceNo)}</span></div>
      <div class="row"><span class="label">Remarks</span><span class="val">${escapeHtml(transaction.remarks1 || '-')}</span></div>
      <div class="row"><span class="label">Date (BS)</span><span class="val">${escapeHtml(transaction.nepaliDate || '-')}</span></div>
      <div class="row"><span class="label">Date (AD)</span><span class="val">${escapeHtml(new Date(paidAt).toLocaleString())}</span></div>
      <div class="row"><span class="label">Reference (PRN)</span><span class="val">${escapeHtml(transaction.prn)}</span></div>
      ${transaction.fonepayTraceId ? `<div class="row"><span class="label">Fonepay Trace ID</span><span class="val">${escapeHtml(transaction.fonepayTraceId)}</span></div>` : ''}
    </div>

    <a class="download" href="/bill/${escapeHtml(transaction.billToken)}/pdf">
      <svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path stroke-linecap="round" stroke-linejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12L12 16.5m0 0L7.5 12m4.5 4.5V3" />
      </svg>
      Download PDF Receipt
    </a>

    <div class="footer-note">This is a computer-generated digital receipt.</div>
  </div>
</body>
</html>`;
}

function streamBillPdf(transaction, res) {
  const paidAt = transaction.paidAt || transaction.updatedAt || transaction.createdAt;
  const doc = new PDFDocument({
    size: 'A4',
    margin: 40,
    info: {
      Title: `Invoice INV-${transaction.invoiceNo}`,
      Author: 'Fonepay QR System',
      Subject: 'Payment Invoice',
    },
  });

  doc.pipe(res);

  const pageWidth = doc.page.width;
  const contentWidth = pageWidth - doc.page.margins.left - doc.page.margins.right;
  const logoPath = require('path').join(__dirname, 'images', 'logo.png');

  const C_PRIMARY = '#1e1b4b';
  const C_BG_LIGHT = '#f8fafc';
  const C_BORDER = '#e2e8f0';
  const C_TEXT_MAIN = '#0f172a';
  const C_TEXT_MUTED = '#64748b';

  doc.roundedRect(40, 40, contentWidth, 85, 12)
    .fill(C_BG_LIGHT);

  try {
    doc.image(logoPath, 58, 53, {
      fit: [60, 60],
      align: 'center',
      valign: 'center',
    });
  } catch (logoErr) {
    console.warn('Could not load invoice logo:', logoErr.message);
  }

  doc.fillColor(C_PRIMARY)
    .font('Helvetica-Bold')
    .fontSize(18)
    .text('TAX INVOICE', 135, 54, { width: 250 });

  doc.fillColor(C_TEXT_MUTED)
    .font('Helvetica')
    .fontSize(8.5)
    .text('Official Fonepay Payment Receipt', 135, 76, { width: 250 });

  doc.fillColor(C_TEXT_MAIN)
    .font('Helvetica-Bold')
    .fontSize(9.5)
    .text(`INV-${transaction.invoiceNo}`, 135, 93, { width: 250 });

  const badgeWidth = 64;
  const badgeHeight = 22;
  const badgeX = pageWidth - 40 - badgeWidth - 18;
  const badgeY = 56;

  doc.roundedRect(badgeX, badgeY, badgeWidth, badgeHeight, badgeHeight / 2)
    .fill('#dcfce7');

  doc.fillColor('#15803d')
    .font('Helvetica-Bold')
    .fontSize(8.5)
    .text('PAID', badgeX, badgeY + 6, {
      width: badgeWidth,
      align: 'center',
    });

  doc.y = 145;

  const cardWidth = 260;
  const cardHeight = 68;
  const cardX = (pageWidth - cardWidth) / 2;
  const cardY = doc.y;

  doc.roundedRect(cardX, cardY, cardWidth, cardHeight, 10)
    .fill(C_PRIMARY);

  doc.fillColor('#94a3b8')
    .font('Helvetica-Bold')
    .fontSize(7.5)
    .text('TOTAL AMOUNT PAID', cardX, cardY + 14, { width: cardWidth, align: 'center', tracking: 1 });

doc.registerFont('NepaliFont', require('path').join(__dirname, 'Fonts', 'NotoSansDevanagari-Regular.ttf'));

doc.fillColor('#ffffff')
  .font('NepaliFont')
  .fontSize(20)
  .text(`रु ${Number(transaction.amount).toLocaleString('en-IN')}`, cardX, cardY + 24, { width: cardWidth, align: 'center' });

  doc.y = cardY + cardHeight + 25;

  doc.fillColor(C_PRIMARY)
    .font('Helvetica-Bold')
    .fontSize(11)
    .text('Transaction Breakdown');

  doc.moveDown(0.6);

  const rows = [
    ['Invoice Reference', `${transaction.invoiceNo}`],
    ['Customer Remarks', transaction.remarks1 || 'None'],
    ['Date (BS)', transaction.nepaliDate || '-'],
    ['Date (AD)', new Date(paidAt).toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' })],
    ['PRN', transaction.prn],
  ];

  if (transaction.fonepayTraceId) {
    rows.push(['Fonepay Trace ID', transaction.fonepayTraceId]);
  }

  const rowHeight = 28;
  const labelX = 55;
  const valueX = 220;
  let rowY = doc.y;

  rows.forEach(([label, value], index) => {
    if (index % 2 === 0) {
      doc.roundedRect(40, rowY - 4, contentWidth, rowHeight, 4)
        .fill(C_BG_LIGHT);
    }

    doc.fillColor(C_TEXT_MUTED)
      .font('Helvetica')
      .fontSize(9)
      .text(label, labelX, rowY + 5, { width: 150 });

    doc.fillColor(C_TEXT_MAIN)
      .font('Helvetica-Bold')
      .fontSize(9)
      .text(String(value), valueX, rowY + 5, {
        width: contentWidth - 190,
        align: 'right',
      });

    rowY += rowHeight;
  });

  doc.y = rowY + 30;

  doc.moveTo(40, doc.y)
    .lineTo(pageWidth - 40, doc.y)
    .lineWidth(0.75)
    .stroke(C_BORDER);

  doc.moveDown(1.2);

  doc.fillColor(C_TEXT_MAIN)
    .font('Helvetica-Bold')
    .fontSize(9.5)
    .text('Thank you for your Purchase!', { align: 'center' });

  doc.moveDown(0.3);

  doc.fillColor(C_TEXT_MUTED)
    .font('Helvetica')
    .fontSize(7.5)
    .text('This is a computer-generated digital receipt and requires no physical signature.', {
      align: 'center',
    });

  doc.end();
}

app.get('/bill/:token', async (req, res) => {
  try {
    const transaction = await Transaction.findOne({
      billToken: req.params.token,
      status: 'paid',
    }).lean();
    if (!transaction) return res.status(404).send('Bill not found');
    res.send(renderBillHtml(transaction));
  } catch (err) {
    console.error('Failed to render bill:', err.message);
    res.status(500).send('Could not load bill');
  }
});

app.get('/bill/:token/pdf', async (req, res) => {
  try {
    const transaction = await Transaction.findOne({
      billToken: req.params.token,
      status: 'paid',
    }).lean();
    if (!transaction) return res.status(404).json({ error: 'Bill not found' });

    res.setHeader('Content-Type', 'application/pdf');
    res.setHeader('Content-Disposition', `attachment; filename="invoice-${transaction.invoiceNo}.pdf"`);
    streamBillPdf(transaction, res);
  } catch (err) {
    console.error('Failed to generate bill PDF:', err.message);
    res.status(500).json({ error: 'Could not generate PDF' });
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

app.post('/api/transactions/:prn/verify', requireAdminKey, async (req, res) => {
  try {
    const transaction = await Transaction.findOne({ prn: req.params.prn });
    if (!transaction) return res.status(404).json({ error: 'Transaction not found' });

    const result = await fonepay.checkStatus(req.params.prn);

    if (result.fonepayTraceId) transaction.fonepayTraceId = String(result.fonepayTraceId);

    if (result.paymentStatus === 'success') {
      transaction.status = 'paid';
      if (!transaction.billToken) {
        transaction.billToken = generateBillToken();
        transaction.paidAt = new Date();
      }
    } else if (result.paymentStatus === 'failed' || result.paymentStatus === 'cancelled') {
      transaction.status = 'failed';
    }

    await transaction.save();

    res.json({
      transaction: transaction.toObject(),
      billUrl: transaction.billToken ? `/bill/${transaction.billToken}` : null,
      fonepayStatus: result.paymentStatus,
    });
  } catch (err) {
    console.error(`Verify failed for ${req.params.prn}:`, err.details || err.message);
    res.status(502).json({ error: 'Could not verify transaction with Fonepay' });
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
