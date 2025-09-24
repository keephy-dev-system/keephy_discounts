import express from 'express';
import mongoose from 'mongoose';
import pino from 'pino';
import pinoHttp from 'pino-http';

const PORT = process.env.PORT || 3006;
const MONGO_URL = process.env.MONGO_URL || 'mongodb://localhost:27017/keephy_discounts';
const logger = pino({ level: process.env.LOG_LEVEL || 'info' });

mongoose.set('strictQuery', true);
mongoose
  .connect(MONGO_URL, { autoIndex: true })
  .then(() => logger.info({ msg: 'Connected to MongoDB', url: MONGO_URL }))
  .catch((err) => {
    logger.error({ err }, 'MongoDB connection error');
    process.exit(1);
  });

const discountSchema = new mongoose.Schema(
  {
    accessKey: { type: String, required: true, unique: true, index: true },
    businessId: { type: mongoose.Schema.Types.ObjectId, required: true, index: true },
    title: String,
    description: String,
    rules: {
      perDeviceCooldownMinutes: { type: Number, default: 1440 },
      perEmailOnce: { type: Boolean, default: true },
    },
    active: { type: Boolean, default: true },
  },
  { timestamps: true }
);

const redemptionSchema = new mongoose.Schema(
  {
    discountId: { type: mongoose.Schema.Types.ObjectId, ref: 'Discount', index: true },
    accessKey: { type: String, index: true },
    businessId: { type: mongoose.Schema.Types.ObjectId, index: true },
    email: { type: String, index: true },
    deviceId: { type: String, index: true },
    ip: { type: String },
    idempotencyKey: { type: String, unique: true, index: true },
  },
  { timestamps: true }
);

const Discount = mongoose.model('Discount', discountSchema);
const Redemption = mongoose.model('Redemption', redemptionSchema);

// Simple Outbox for discount events
const outboxSchema = new mongoose.Schema(
  {
    type: { type: String, required: true },
    payload: { type: Object, required: true },
    status: { type: String, enum: ['pending', 'sent', 'failed'], default: 'pending', index: true },
    attempts: { type: Number, default: 0 },
    lastError: { type: String },
  },
  { timestamps: true }
);
const Outbox = mongoose.model('DiscountOutbox', outboxSchema);

const app = express();
app.use(express.json());
app.use(pinoHttp({ logger }));

app.get('/health', (_req, res) => res.json({ status: 'ok', service: 'discounts-service' }));
app.get('/ready', (_req, res) => {
  const state = mongoose.connection.readyState; // 1 = connected
  res.status(state === 1 ? 200 : 503).json({ ready: state === 1 });
});

// GET /discounts/:accessKey → resolve discount (public)
app.get('/discounts/:accessKey', async (req, res) => {
  try {
    const d = await Discount.findOne({ accessKey: req.params.accessKey, active: true }).lean();
    if (!d) return res.status(404).json({ message: 'Not found' });
    res.json(d);
  } catch (err) {
    req.log.error({ err }, 'discount fetch failed');
    res.status(500).json({ message: 'Internal server error' });
  }
});

// POST /discounts/mark-used { accessKey, email?, deviceId?, idempotencyKey }
app.post('/discounts/mark-used', async (req, res) => {
  try {
    const { accessKey, email, deviceId, idempotencyKey } = req.body || {};
    if (!accessKey || !idempotencyKey) return res.status(400).json({ message: 'accessKey and idempotencyKey required' });

    const d = await Discount.findOne({ accessKey, active: true });
    if (!d) return res.status(404).json({ message: 'Discount not found' });

    // Idempotency first
    const existing = await Redemption.findOne({ idempotencyKey }).lean();
    if (existing) return res.status(200).json({ status: 'ok', redemptionId: existing._id });

    // Enforcement: email once
    if (email && d.rules?.perEmailOnce) {
      const prior = await Redemption.findOne({ discountId: d._id, email });
      if (prior) return res.status(409).json({ message: 'Already redeemed with this email' });
    }
    // Enforcement: device cooldown
    if (deviceId && d.rules?.perDeviceCooldownMinutes) {
      const since = new Date(Date.now() - d.rules.perDeviceCooldownMinutes * 60 * 1000);
      const recent = await Redemption.findOne({ discountId: d._id, deviceId, createdAt: { $gte: since } });
      if (recent) return res.status(429).json({ message: 'Device cooldown active' });
    }

    const red = await Redemption.create({
      discountId: d._id,
      accessKey,
      businessId: d.businessId,
      email,
      deviceId,
      ip: req.ip,
      idempotencyKey,
    });

    // queue event
    await Outbox.create({ type: 'DiscountClaimed', payload: { redemptionId: red._id, discountId: d._id, businessId: d.businessId, email, deviceId, at: red.createdAt }, status: 'pending' });

    res.status(201).json({ status: 'ok', redemptionId: red._id });
  } catch (err) {
    if (err?.code === 11000) {
      // duplicate idempotencyKey
      const again = await Redemption.findOne({ idempotencyKey: req.body?.idempotencyKey }).lean();
      return res.status(200).json({ status: 'ok', redemptionId: again?._id });
    }
    req.log.error({ err }, 'redeem failed');
    res.status(500).json({ message: 'Internal server error' });
  }
});

app.listen(PORT, () => logger.info(`discounts-service listening on ${PORT}`));

// Pending endpoint for debugging
app.get('/outbox/pending', async (_req, res) => {
  const items = await Outbox.find({ status: 'pending' }).sort({ createdAt: 1 }).limit(50).lean();
  res.json({ items });
});

app.post('/internal/consume-outbox', async (req, res) => {
  const limit = Math.min(Number(req.body?.limit || 10), 100);
  const processed = [];
  for (let i = 0; i < limit; i += 1) {
    // eslint-disable-next-line no-await-in-loop
    const doc = await Outbox.findOneAndUpdate(
      { status: 'pending' },
      { $set: { status: 'sent' }, $inc: { attempts: 1 } },
      { sort: { createdAt: 1 }, new: true }
    ).lean();
    if (!doc) break;
    processed.push(doc._id);
  }
  res.json({ processedCount: processed.length, processed });
});

// Background dispatcher stub
const DISPATCH_INTERVAL_MS = Number(process.env.DISPATCH_INTERVAL_MS || 5000);
setInterval(async () => {
  try {
    const doc = await Outbox.findOneAndUpdate(
      { status: 'pending' },
      { $set: { status: 'sent' }, $inc: { attempts: 1 } },
      { sort: { createdAt: 1 }, new: true }
    ).lean();
    if (doc) {
      // would publish to bus
      logger.info({ type: doc.type, id: doc._id }, 'Dispatched discount event (stub)');
    }
  } catch (err) {
    logger.error({ err }, 'Outbox dispatcher error');
  }
}, DISPATCH_INTERVAL_MS);


