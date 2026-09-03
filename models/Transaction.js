const mongoose = require('mongoose');
const NepaliDate = require('nepali-date');

const transactionSchema = new mongoose.Schema(
  {
    prn: { type: String, required: true, unique: true, index: true },
    amount: { type: Number, required: true },
    remarks1: { type: String, default: '' },
    remarks2: { type: String, default: '' },
    invoiceNo: { type: String, default: '', index: true },
    status: {
      type: String,
      enum: ['pending', 'qr_scanned', 'paid', 'failed', 'timeout'],
      default: 'pending',
      index: true,
    },
    qrMessage: { type: String, default: '' },
    // Only Nepali creation date (no expiry, no trace ID)
    nepaliDate: { type: String, default: '' },
  },
  { timestamps: true }
);

function pad(n) {
  return String(n).padStart(2, '0');
}

transactionSchema.pre('save', function (next) {
  if (this.isNew) {
    try {
      const now = new Date();
      const nepaliNow = new NepaliDate(now);
      this.nepaliDate =
        `${nepaliNow.getYear()}-${pad(nepaliNow.getMonth() + 1)}-${pad(nepaliNow.getDate())} ` +
        `${pad(nepaliNow.getHours())}:${pad(nepaliNow.getMinutes())}:${pad(nepaliNow.getSeconds())}`;
    } catch (err) {
      console.warn('Nepali date conversion failed:', err.message);
    }
  }
  next();
});

module.exports = mongoose.model('Transaction', transactionSchema);
