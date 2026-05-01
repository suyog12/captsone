import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import {
  ShoppingCart,
  Package,
  Search,
  Layers,
  Calendar as CalendarIcon,
  PiggyBank
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import ReorderHeatmap from '../../components/customer/ReorderHeatmap.jsx';
import CategoryDonut from '../../components/customer/CategoryDonut.jsx';
import InventoryGauge from '../../components/customer/InventoryGauge.jsx';
import SuggestedCarousel from '../../components/customer/SuggestedCarousel.jsx';
import {
  getMyCustomerRecord,
  getMyRecommendations,
  getMyCart,
  getCustomerHistory,
  getCustomerStats,
  getCartHistory
} from '../../api.js';
import { formatCurrency, formatNumber, formatDate } from '../../lib/format.js';
import { useAuth } from '../../auth.jsx';

// Customer overview

// Customer-facing dashboard. Deliberately avoids any seller-only language
// like "high value", "at risk", "churn risk", "declining" - those tags
// belong to the seller's view of the same customer, not the customer's
// own view of themselves. The KPI strip uses neutral, helpful framing:
// orders this year, top product family, last order, items in cart.

export default function CustomerOverview() {
  const navigate = useNavigate();
  const { user } = useAuth();
  const myCustId = user && user.cust_id;

  const customerQuery = useQuery({
    queryKey: ['customer', 'me'],
    queryFn: getMyCustomerRecord
  });

  // Pull 6 recs for the carousel (top 3 still highlighted via list ordering).
  const recsQuery = useQuery({
    queryKey: ['customer', 'me', 'recs', 6],
    queryFn: () => getMyRecommendations(6)
  });

  const cartQuery = useQuery({
    queryKey: ['cart', 'me'],
    queryFn: getMyCart
  });

  const historyQuery = useQuery({
    queryKey: ['customer', 'me', 'history-preview'],
    queryFn: () => {
      if (!myCustId) return Promise.resolve({ items: [] });
      return getCustomerHistory(myCustId, { limit: 5 });
    },
    enabled: Boolean(myCustId)
  });

  // Customer stats over the last year - drives top family, orders count,
  // and feeds the donut + inventory gauge.
  const statsQuery = useQuery({
    queryKey: ['customer', 'me', 'stats', '1y'],
    queryFn: () => getCustomerStats(myCustId, { range: '1y', granularity: 'monthly', top_families: 6 }),
    enabled: Boolean(myCustId)
  });

// Cart history for the year, filtered to PB-upgrade conversions, drives
  // the optional "Estimated savings YTD" tile.
  const ytdCartQuery = useQuery({
    queryKey: ['customer', 'me', 'cart-history-ytd'],
    queryFn: () => getCartHistory(myCustId, { status_filter: 'sold', limit: 500 }),
    enabled: Boolean(myCustId)
  });

  // Optional savings: sum line totals across pb_upgrade conversions in the
  // past year. We approximate "savings" as 12% of the line total - this is a
  // rough proxy and we label it "estimated" to be honest about it. Toggle
  // off entirely if no PB-upgrade conversions exist.
  const ytdSavings = useMemo(() => {
    const items = (ytdCartQuery.data && ytdCartQuery.data.items) || [];
    let savedRevenue = 0;
    for (let i = 0; i < items.length; i = i + 1) {
      const it = items[i];
      if (it.source === 'recommendation_pb_upgrade') {
        const lineTotal =
          parseFloat(it.line_total) ||
          (parseFloat(it.unit_price_at_add) || 0) * (it.quantity || 0);
        savedRevenue = savedRevenue + lineTotal;
      }
    }
    // Conservative 12% savings estimate vs national-brand equivalent
    return savedRevenue * 0.12;
  }, [ytdCartQuery.data]);

  if (customerQuery.isLoading) {
    return (
      <AppShell title="Loading">
        <FullPanelSpinner label="Loading your dashboard" />
      </AppShell>
    );
  }
  
  const customer = customerQuery.data;
  const recs = (recsQuery.data && recsQuery.data.recommendations) || [];
  const cart = cartQuery.data;
  const cartItems = (cart && cart.items) || [];
  const recentLines = (historyQuery.data && historyQuery.data.items) || [];

  const stats = statsQuery.data;
  const summary = (stats && stats.summary) || {};
  const topFamilies = (stats && stats.top_families) || [];

  // KPI values
  const ordersYear = summary.total_orders || 0;
  const topFamilyName = topFamilies.length > 0 ? topFamilies[0].family : null;
  const lastOrderDate = summary.last_order_date || null;
  const cartItemCount = cartItems.length;



  return (
    <AppShell
      title={`Welcome, ${(user && user.full_name) || (customer && customer.customer_name) || (user && user.username) || ''}`}
      subtitle={customer ? customer.customer_name : ''}
    >
      <div className="space-y-6">
        {/* Account header card - no lifecycle/risk badges in customer view */}
        {customer ? (
          <Card padding="lg">
            <div className="flex items-start justify-between gap-3 flex-wrap">
              <div>
                <h2 className="text-base font-semibold text-mck-navy">{customer.customer_name}</h2>
                <p className="text-xs text-slate-500 mt-1">
                  {customer.segment} &middot; Specialty {customer.specialty_code} &middot; #{customer.cust_id}
                </p>
              </div>
              <button
                type="button"
                onClick={() => navigate('/customer/catalog')}
                className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-mck-blue text-white rounded hover:bg-mck-blue-dark"
              >
                <Search size={12} />
                Browse catalog
              </button>
            </div>
          </Card>
        ) : null}

        {/* KPI strip - neutral, customer-friendly framing */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Orders this year"
            value={statsQuery.isLoading ? '...' : formatNumber(ordersYear)}
            hint={summary.total_items_purchased ? `${formatNumber(summary.total_items_purchased)} items total` : 'across all orders'}
            icon={CalendarIcon}
            accent="mck-blue"
          />
          <StatCard
            label="Top product family"
            value={statsQuery.isLoading ? '...' : (topFamilyName || '-')}
            hint={
              topFamilies.length > 0 && topFamilies[0].pct_of_total_revenue
                ? `${topFamilies[0].pct_of_total_revenue.toFixed(0)}% of your spend`
                : 'most-shopped category'
            }
            icon={Layers}
            accent="mck-orange"
          />
          <StatCard
            label="Last order"
            value={lastOrderDate ? formatDate(lastOrderDate) : 'No orders yet'}
            hint={lastOrderDate ? 'most recent purchase' : 'start with the catalog'}
            icon={Package}
            accent="mck-navy"
          />
          <StatCard
            label="Items in cart"
            value={formatNumber(cartItemCount)}
            hint={cart ? formatCurrency(cart.estimated_total) : '$0.00'}
            icon={ShoppingCart}
            accent="green"
          />
        </div>

        {/* Optional Estimated savings tile - shown only if non-zero */}
        {ytdSavings > 1 ? (
          <Card padding="md" className="border-l-4 border-l-mck-orange">
            <div className="flex items-center gap-3 flex-wrap">
              <div className="flex-shrink-0 w-10 h-10 rounded-md bg-mck-orange/10 flex items-center justify-center">
                <PiggyBank size={20} className="text-mck-orange" />
              </div>
              <div className="flex-1 min-w-[12rem]">
                <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">
                  Estimated savings YTD
                </div>
                <div className="text-lg font-bold text-mck-navy">
                  {formatCurrency(ytdSavings)}
                </div>
                <div className="text-[11px] text-slate-500 mt-0.5">
                  From McKesson Brand swaps versus national-brand equivalents (estimate).
                </div>
              </div>
            </div>
          </Card>
        ) : null}

        {/* Suggested for you carousel - top 6 recs */}
        <SuggestedCarousel
          recs={recs}
          custId={myCustId}
          onSeeAll={() => navigate('/customer/recommendations')}
        />

        {/* Visual density row 1: heatmap + donut */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <ReorderHeatmap custId={myCustId} />
          <CategoryDonut topFamilies={topFamilies} isLoading={statsQuery.isLoading} />
        </div>

        {/* Visual density row 2: low stock gauges + recent purchases */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <InventoryGauge
            custId={myCustId}
            topFamilies={topFamilies}
            isLoading={statsQuery.isLoading}
          />

          <Card padding="none">
            <div className="px-5 pt-5 pb-3 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-mck-navy">Recent purchases</h3>
              <button
                type="button"
                onClick={() => navigate('/customer/orders')}
                className="text-xs font-semibold text-mck-blue hover:text-mck-blue-dark inline-flex items-center gap-1"
              >
                All orders
              </button>
            </div>
            {historyQuery.isLoading ? (
              <div className="px-5 pb-5"><FullPanelSpinner label="Loading" /></div>
            ) : recentLines.length === 0 ? (
              <div className="px-5 pb-5 text-center text-xs text-slate-500 py-6">
                No recent purchases yet.
              </div>
            ) : (
              <ul className="px-5 pb-4 space-y-2">
                {recentLines.slice(0, 5).map((line, idx) => (
                  <li key={idx} className="flex items-start gap-2 text-xs">
                    <Package size={12} className="text-slate-400 mt-0.5 flex-shrink-0" />
                    <div className="flex-1 min-w-0">
                      <div className="font-medium text-mck-navy truncate">
                        {line.description || `Item ${line.item_id}`}
                      </div>
                      <div className="text-slate-500 mt-0.5">
                        {formatDate(line.sold_at)} &middot; Qty {line.quantity} &middot; {formatCurrency(line.unit_price)}
                      </div>
                    </div>
                    <div className="text-mck-navy font-semibold whitespace-nowrap">
                      {formatCurrency(line.line_total)}
                    </div>
                  </li>
                ))}
              </ul>
            )}
          </Card>
        </div>

        {/* Cart preview at the bottom for quick checkout context */}
        {cartItems.length > 0 ? (
          <Card padding="none">
            <div className="px-5 pt-5 pb-3 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-mck-navy">Cart preview</h3>
              <button
                type="button"
                onClick={() => navigate('/customer/cart')}
                className="text-xs font-semibold text-mck-blue hover:text-mck-blue-dark"
              >
                Open cart
              </button>
            </div>
            <div className="px-5 pb-4 space-y-2">
              {cartItems.slice(0, 3).map((item) => (
                <div key={item.cart_item_id} className="flex items-center justify-between text-xs">
                  <div className="flex-1 min-w-0">
                    <div className="font-medium text-mck-navy truncate">
                      {item.description || `Item ${item.item_id}`}
                    </div>
                    <div className="text-slate-500 mt-0.5">
                      Qty {item.quantity} &middot; {formatCurrency(item.unit_price_at_add)}
                    </div>
                  </div>
                  <div className="text-mck-navy font-semibold pl-2">
                    {formatCurrency((item.unit_price_at_add || 0) * (item.quantity || 0))}
                  </div>
                </div>
              ))}
              {cartItems.length > 3 ? (
                <div className="text-xs text-slate-500 italic">
                  and {cartItems.length - 3} more...
                </div>
              ) : null}
              <div className="border-t border-slate-100 pt-2 mt-2 flex items-center justify-between text-sm">
                <div className="text-slate-600 font-medium">Estimated total</div>
                <div className="text-mck-navy font-bold">
                  {cart ? formatCurrency(cart.estimated_total) : '$0.00'}
                </div>
              </div>
            </div>
          </Card>
        ) : null}
      </div>
    </AppShell>
  );
}
