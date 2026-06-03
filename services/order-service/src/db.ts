import mongoose from 'mongoose';

const orderSchema = new mongoose.Schema({
  restaurantId: { type: String, required: true },
  items: [String],
  status: { 
    type: String, 
    enum: ['CREATED', 'ACCEPTED', 'READY', 'PICKED_UP', 'DELIVERED'],
    default: 'CREATED'
  },
  location: {
    lat: Number,
    lng: Number
  },
  createdAt: { type: Date, default: Date.now }
});

// Compound Index for optimized restaurant order management
orderSchema.index({ restaurantId: 1, status: 1 });

export const Order = mongoose.model('Order', orderSchema);

export async function connectDB() {
  const uri = process.env.MONGO_URI || 'mongodb://localhost:27017/orders';
  await mongoose.connect(uri);
  console.log('Connected to MongoDB');
}
