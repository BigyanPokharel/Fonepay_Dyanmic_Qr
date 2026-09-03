const crypto = require('crypto');
const fetch = require('node-fetch');

const MAX_PRN_LENGTH = 25;
const MAX_REMARKS_LENGTH = 25;

function sanitizeForFonepay(value, maxLength) {
  return String(value ?? '')
    .replace(/,/g, ' ')
    .trim()
    .slice(0, maxLength);
}

class FonepayService {
  constructor(config) {
    this.merchantCode = config.merchantCode;
    this.secretKey = config.secretKey;
    this.username = config.username;
    this.password = config.password;
    this.baseUrl = config.baseUrl;

    const missing = ['merchantCode', 'secretKey', 'username', 'password', 'baseUrl'].filter(
      (key) => !this[key]
    );
    if (missing.length) {
      console.warn(
        `[FonepayService] Missing config: ${missing.join(', ')}. ` +
          `Check your .env file — requests will likely fail until these are set.`
      );
    }
  }

  _sign(message) {
    return crypto
      .createHmac('sha512', this.secretKey)
      .update(message, 'utf8')
      .digest('hex');
  }

  async _postJson(url, body) {
    let res;
    try {
      res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
    } catch (networkErr) {
      const err = new Error(`Network error calling Fonepay: ${networkErr.message}`);
      err.cause = networkErr;
      throw err;
    }

    const rawText = await res.text();

    let data;
    try {
      data = JSON.parse(rawText);
    } catch {
      const err = new Error(
        `Fonepay returned a non-JSON response (status ${res.status} ${res.statusText}). ` +
          `First 300 chars: ${rawText.slice(0, 300)}`
      );
      err.status = res.status;
      err.rawBody = rawText;
      err.details = { status: res.status, statusText: res.statusText, rawBody: rawText.slice(0, 1000) };
      throw err;
    }

    if (!res.ok) {
      const err = new Error(data.message || `Fonepay request failed (status ${res.status})`);
      err.status = res.status;
      err.details = data;
      throw err;
    }

    return data;
  }

  async generateQr({ amount, prn, remarks1 = 'Order', remarks2 = '-' }) {
    const amountStr = String(amount);
    const safePrn = sanitizeForFonepay(prn, MAX_PRN_LENGTH);
    const safeRemarks1 = sanitizeForFonepay(remarks1, MAX_REMARKS_LENGTH);
    const safeRemarks2 = sanitizeForFonepay(remarks2, MAX_REMARKS_LENGTH);

    const message = [amountStr, safePrn, this.merchantCode, safeRemarks1, safeRemarks2].join(',');
    const dataValidation = this._sign(message);

    const body = {
      amount: amountStr,
      remarks1: safeRemarks1,
      remarks2: safeRemarks2,
      prn: safePrn,
      merchantCode: this.merchantCode,
      dataValidation,
      username: this.username,
      password: this.password,
    };

    const data = await this._postJson(
      `${this.baseUrl}/merchant/merchantDetailsForThirdParty/thirdPartyDynamicQrDownload`,
      body
    );

    if (data.success !== true) {
      const err = new Error(data.message || 'Fonepay QR generation failed');
      err.details = data;
      throw err;
    }

    return data;
  }

  async checkStatus(prn) {
    const safePrn = sanitizeForFonepay(prn, MAX_PRN_LENGTH);
    const message = [safePrn, this.merchantCode].join(',');
    const dataValidation = this._sign(message);

    const body = {
      prn: safePrn,
      merchantCode: this.merchantCode,
      dataValidation,
      username: this.username,
      password: this.password,
    };

    return this._postJson(
      `${this.baseUrl}/merchant/merchantDetailsForThirdParty/thirdPartyDynamicQrGetStatus`,
      body
    );
  }
}

module.exports = FonepayService;