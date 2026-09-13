const mongoose = require('mongoose');

const MONGODB_URI_FONEPAY = process.env.MONGODB_URI_FONEPAY || 'mongodb://127.0.0.1:27017/fonepay_qr';

const mongooseOptions = MONGODB_URI_FONEPAY.startsWith('mongodb+srv://')
  ? {
      serverApi: {
        version: mongoose.mongo.ServerApiVersion.v1,
        strict: true,
        deprecationErrors: true,
      },
    }
  : {};

async function connectDatabase() {
  try {
    await mongoose.connect(MONGODB_URI_FONEPAY, mongooseOptions);
    const dbType = MONGODB_URI_FONEPAY.startsWith('mongodb+srv://') ? 'Atlas' : 'Local';
    console.log(`Connected to MongoDB (${dbType})`);
  } catch (err) {
    console.error('MongoDB connection failed:', err.message);
    process.exit(1);
  }
}

module.exports = { connectDatabase };
