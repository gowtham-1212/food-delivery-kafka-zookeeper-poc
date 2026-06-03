import { useEffect, useState, useRef } from 'react';
import { Terminal, Package, CheckCircle, Truck, Utensils, PlusCircle, Send, Loader2 } from 'lucide-react';

interface KafkaEvent {
  topic: string;
  payload: any;
  timestamp: string;
}

interface OrderStatus {
  orderId: string;
  status: 'CREATED' | 'ACCEPTED' | 'READY' | 'PICKED_UP' | 'DELIVERED';
}

function App() {
  const [events, setEvents] = useState<KafkaEvent[]>([]);
  const [orders, setOrders] = useState<Record<string, OrderStatus>>({});
  const [restaurantId, setRestaurantId] = useState('rest_001');
  const [items, setItems] = useState('Margherita Pizza, Coke');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const terminalEndRef = useRef<HTMLDivElement>(null);

  const TRACKING_SERVICE_URL = import.meta.env.VITE_TRACKING_SERVICE_URL || 'http://localhost:3004';
  const ORDER_SERVICE_URL = import.meta.env.VITE_ORDER_SERVICE_URL || 'http://localhost:3000';

  useEffect(() => {
    const sse = new EventSource(`${TRACKING_SERVICE_URL}/api/stream`);

    sse.onmessage = (e) => {
      const event: KafkaEvent = JSON.parse(e.data);
      setEvents((prev) => [...prev.slice(-49), event]); // Keep last 50 events

      if (event.payload.orderId) {
        setOrders((prev) => ({
          ...prev,
          [event.payload.orderId]: {
            orderId: event.payload.orderId,
            status: event.payload.status || 'CREATED'
          }
        }));
      }
    };

    return () => sse.close();
  }, []);

  useEffect(() => {
    terminalEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [events]);

  const handlePlaceOrder = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsSubmitting(true);
    try {
      const response = await fetch(`${ORDER_SERVICE_URL}/api/orders`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          restaurantId,
          items: items.split(',').map(i => i.trim()),
          location: { lat: 40.7128, lng: -74.0060 } // Default NYC location
        })
      });
      if (!response.ok) throw new Error('Failed to place order');
      const data = await response.json();
      console.log('Order placed:', data);
    } catch (err) {
      console.error(err);
      alert('Error placing order. Make sure order-service is running.');
    } finally {
      setIsSubmitting(false);
    }
  };

  const getStatusIndex = (status: string) => {
    const steps = ['CREATED', 'ACCEPTED', 'READY', 'PICKED_UP', 'DELIVERED'];
    return steps.indexOf(status);
  };

  return (
    <div className="min-h-screen p-8 font-sans">
      <header className="mb-8 border-b border-slate-800 pb-4">
        <h1 className="text-3xl font-bold text-orange-500">Food Delivery POC Dashboard</h1>
        <p className="text-slate-400">Real-time event tracking via Kafka & SSE</p>
      </header>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">
        
        {/* Left Column: Form & Active Orders */}
        <div className="xl:col-span-2 space-y-8">
          
          {/* Order Placement Form */}
          <div className="bg-slate-900 rounded-xl p-6 border border-slate-800 shadow-2xl">
            <h2 className="text-xl font-semibold mb-6 flex items-center gap-2 text-orange-400">
              <PlusCircle size={20} /> Place New Order
            </h2>
            <form onSubmit={handlePlaceOrder} className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="flex flex-col gap-1">
                <label className="text-xs text-slate-500 uppercase font-bold">Restaurant ID</label>
                <input 
                  type="text" 
                  value={restaurantId}
                  onChange={(e) => setRestaurantId(e.target.value)}
                  className="bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm focus:outline-none focus:border-orange-500"
                  placeholder="e.g. rest_123"
                  required
                />
              </div>
              <div className="flex flex-col gap-1 md:col-span-1">
                <label className="text-xs text-slate-500 uppercase font-bold">Items (comma separated)</label>
                <input 
                  type="text" 
                  value={items}
                  onChange={(e) => setItems(e.target.value)}
                  className="bg-slate-800 border border-slate-700 rounded px-3 py-2 text-sm focus:outline-none focus:border-orange-500"
                  placeholder="Pizza, Coke, Fries"
                  required
                />
              </div>
              <div className="flex items-end">
                <button 
                  type="submit" 
                  disabled={isSubmitting}
                  className="w-full bg-orange-600 hover:bg-orange-500 disabled:bg-slate-700 text-white font-bold py-2 px-4 rounded transition-colors flex items-center justify-center gap-2"
                >
                  {isSubmitting ? <Loader2 className="animate-spin" size={18} /> : <Send size={18} />}
                  {isSubmitting ? 'Placing...' : 'Submit Order'}
                </button>
              </div>
            </form>
          </div>

          {/* Active Orders Section */}
          <div className="bg-slate-900 rounded-xl p-6 border border-slate-800 shadow-2xl">
            <h2 className="text-xl font-semibold mb-6 flex items-center gap-2">
              <Package className="text-blue-400" /> Active Orders
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {Object.values(orders).length === 0 && (
                <p className="text-slate-500 italic col-span-2">No active orders. Use the form above to place one!</p>
              )}
              {Object.values(orders).sort((a, b) => b.orderId.localeCompare(a.orderId)).map((order) => (
                <div key={order.orderId} className="bg-slate-800/50 p-4 rounded-lg border border-slate-700">
                  <div className="flex justify-between mb-4">
                    <span className="text-xs font-mono text-slate-400">ID: {order.orderId.slice(-6)}</span>
                    <span className={`text-xs font-bold px-2 py-0.5 rounded ${
                      order.status === 'DELIVERED' ? 'bg-green-900/50 text-green-400' : 'bg-blue-900/50 text-blue-400'
                    }`}>
                      {order.status}
                    </span>
                  </div>
                  
                  {/* Progress Bar */}
                  <div className="relative flex justify-between">
                    {['CREATED', 'ACCEPTED', 'READY', 'PICKED_UP', 'DELIVERED'].map((step, idx) => (
                      <div key={step} className="flex flex-col items-center z-10">
                        <div className={`w-6 h-6 rounded-full flex items-center justify-center border-2 ${
                          getStatusIndex(order.status) >= idx 
                          ? 'bg-orange-500 border-orange-500 text-white' 
                          : 'bg-slate-800 border-slate-600 text-slate-500'
                        }`}>
                          {idx === 0 && <Package size={10} />}
                          {idx === 1 && <Utensils size={10} />}
                          {idx === 2 && <CheckCircle size={10} />}
                          {idx === 3 && <Truck size={10} />}
                          {idx === 4 && <CheckCircle size={10} />}
                        </div>
                      </div>
                    ))}
                    {/* Progress Line */}
                    <div className="absolute top-3 left-0 w-full h-0.5 bg-slate-700 -z-0"></div>
                    <div 
                      className="absolute top-3 left-0 h-0.5 bg-orange-500 transition-all duration-500 -z-0"
                      style={{ width: `${(getStatusIndex(order.status) / 4) * 100}%` }}
                    ></div>
                  </div>
                  <div className="flex justify-between mt-2">
                    <span className="text-[8px] text-slate-500 uppercase">Ordered</span>
                    <span className="text-[8px] text-slate-500 uppercase">Delivered</span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Right Column: Kafka Logs */}
        <div className="bg-black rounded-xl border border-slate-800 flex flex-col h-[700px] overflow-hidden shadow-2xl xl:col-span-1">
          <div className="bg-slate-900 px-4 py-2 border-b border-slate-800 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Terminal size={16} className="text-green-500" />
              <span className="text-sm font-mono font-bold">KAFKA_LIVE_LOGS</span>
            </div>
            <div className="flex gap-1.5">
              <div className="w-3 h-3 rounded-full bg-red-500"></div>
              <div className="w-3 h-3 rounded-full bg-yellow-500"></div>
              <div className="w-3 h-3 rounded-full bg-green-500"></div>
            </div>
          </div>
          <div className="flex-1 overflow-y-auto p-4 font-mono text-[10px] space-y-2">
            {events.map((ev, i) => (
              <div key={i} className="animate-in fade-in slide-in-from-left-2 duration-300 border-l border-slate-800 pl-2">
                <span className="text-gray-500">[{ev.timestamp.split('T')[1].split('.')[0]}]</span>{' '}
                <span className="text-blue-400 font-bold">{ev.topic}</span>
                <div className="text-green-400/80 break-all">{JSON.stringify(ev.payload)}</div>
              </div>
            ))}
            <div ref={terminalEndRef} />
          </div>
        </div>

      </div>
    </div>
  );
}

export default App;
