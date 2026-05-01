import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import {
  Sparkles,
  ShoppingCart,
  ClipboardList,
  ArrowRight,
  TrendingUp,
  Package,
  AlertCircle,
  DollarSign,
  Search
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card, { CardHeader } from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import LifecycleBadge from '../../components/ui/LifecycleBadge.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import LineChart from '../../components/charts/LineChart.jsx';
import RecommendationCard from '../../components/recs/RecommendationCard.jsx';
import {
  getMyCustomerRecord,
  getMyRecommendations,
  getMyCart,
  getCustomerHistory,
  getCustomerStats
} from '../../api.js';
import { formatCurrency, formatNumber, formatDate } from '../../lib/format.js';
import { useAuth } from '../../auth.jsx';

// Customer overview

export default function CustomerOverview() {
  const navigate = useNavigate();
  const { user } = useAuth();
  const myCustId = user && user.cust_id;

  const customerQuery = useQuery({
    queryKey: ['customer', 'me'],
    queryFn: getMyCustomerRecord
  });

  const recsQuery = useQuery({
    queryKey: ['customer', 'me', 'recs', 3],
    queryFn: () => getMyRecommendations(3)
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

  // Spend trend over the last 90d for the new "Total spend" tile + sparkline
  const statsQuery = useQuery({
    queryKey: ['customer', 'me', 'stats', '1y', 'daily'],
    queryFn: () => getCustomerStats(myCustId, { range: '1y', granularity: 'daily' }),
    enabled: Boolean(myCustId)
  });

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

  // Derive Total spend (1y) and trend buckets from the stats response
  const stats = statsQuery.data;
  const trendBuckets = (stats && Array.isArray(stats.trend) ? stats.trend : []).map((row) => ({
    label: row.bucket,
    value: parseFloat(row.revenue) || 0
  }));
  const totalSpend90d = (stats && stats.summary && stats.summary.total_revenue) || 0;
  const orders1y = (stats && stats.summary && stats.summary.total_orders) || 0;

  return (
    <AppShell
      title={`Welcome, ${(user && user.full_name) || (customer && customer.customer_name) || (user && user.username) || ''}`}
      subtitle={customer ? customer.customer_name : ''}
    >
      <div className="space-y-6">
        {customer ? (
          <Card padding="lg">
            <div className="flex items-start justify-between gap-3 flex-wrap">
              <div>
                <div className="flex items-center gap-2 flex-wrap">
                  <h2 className="text-base font-semibold text-mck-navy">{customer.customer_name}</h2>
                  <LifecycleBadge status={customer.status} />
                </div>
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

        {/* KPI strip — Account Status replaced with Total spend (1y) */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Cart items"
            value={formatNumber(cartItems.length)}
            hint={cart ? formatCurrency(cart.estimated_total) : '$0.00'}
            icon={ShoppingCart}
            accent="mck-blue"
          />
          <StatCard
            label="Recommendations"
            value={formatNumber(recs.length)}
            hint="curated for you"
            icon={Sparkles}
            accent="mck-orange"
          />
          <StatCard
            label="Recent orders"
            value={formatNumber(recentLines.length)}
            hint="last 5 lines"
            icon={ClipboardList}
            accent="mck-navy"
          />
          <StatCard
            label="Total spend (1y)"
            value={statsQuery.isLoading ? '...' : formatCurrency(totalSpend90d)}
            hint={statsQuery.isLoading ? 'Loading' : `${formatNumber(orders1y)} orders`}
            icon={DollarSign}
            accent="green"
          />
        </div>

        {/* Spend trend chart (the new graph piece) */}
        <Card padding="none">
          <div className="px-5 pt-5 pb-2 flex items-center justify-between flex-wrap gap-2">
            <div>
              <h3 className="text-sm font-semibold text-mck-navy">Spend trend</h3>
              <p className="text-xs text-slate-500 mt-0.5">Daily spend over the last year</p>
            </div>
            <div className="text-xs text-slate-500">
              {stats && stats.range_label ? stats.range_label : ''}
            </div>
          </div>
          <div className="px-5 pb-5">
            {statsQuery.isLoading ? (
              <div className="h-[200px] flex items-center justify-center text-xs text-slate-400">
                Loading spend trend...
              </div>
            ) : trendBuckets.length === 0 ? (
              <div className="h-[200px] flex items-center justify-center text-xs text-slate-400">
                No spend recorded yet.
              </div>
            ) : (
              <LineChart
                buckets={trendBuckets}
                valueLabel="Revenue"
                formatValue={(v) => formatCurrency(v)}
                color="#1B6BBE"
                height={220}
                fill={1}
              />
            )}
          </div>
        </Card>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2">
            <Card padding="none">
              <div className="px-5 pt-5 pb-3 flex items-center justify-between">
                <div>
                  <h3 className="text-sm font-semibold text-mck-navy">Top recommendations</h3>
                  <p className="text-xs text-slate-500 mt-0.5">Hand-picked for your specialty and segment</p>
                </div>
                <button
                  type="button"
                  onClick={() => navigate('/customer/recommendations')}
                  className="text-xs font-semibold text-mck-blue hover:text-mck-blue-dark inline-flex items-center gap-1"
                >
                  See all <ArrowRight size={12} />
                </button>
              </div>
              {recsQuery.isLoading ? (
                <FullPanelSpinner label="Loading" />
              ) : recs.length === 0 ? (
                <div className="px-5 pb-6">
                  <EmptyState
                    icon={Sparkles}
                    title="No recommendations yet"
                    description="Your recommendations will appear here as your purchase patterns build."
                  />
                </div>
              ) : (
                <div className="px-5 pb-5 space-y-3">
                  {recs.map((rec, idx) => (
                    <RecommendationCard
                      key={rec.item_id || idx}
                      rec={rec}
                      rank={idx + 1}
                      custId={myCustId}
                      compact={1}
                    />
                  ))}
                </div>
              )}
            </Card>
          </div>

          <div className="space-y-4">
            <Card padding="none">
              <div className="px-5 pt-5 pb-3 flex items-center justify-between">
                <h3 className="text-sm font-semibold text-mck-navy">Cart preview</h3>
                <button
                  type="button"
                  onClick={() => navigate('/customer/cart')}
                  className="text-xs font-semibold text-mck-blue hover:text-mck-blue-dark inline-flex items-center gap-1"
                >
                  Open <ArrowRight size={12} />
                </button>
              </div>
              {cartQuery.isLoading ? (
                <div className="px-5 pb-5"><FullPanelSpinner label="Loading" /></div>
              ) : cartItems.length === 0 ? (
                <div className="px-5 pb-5 text-center">
                  <ShoppingCart size={20} className="text-slate-300 mx-auto mb-2" />
                  <div className="text-sm font-semibold text-mck-navy">Cart is empty</div>
                  <div className="text-xs text-slate-500 mt-1">Browse recommendations to fill your cart.</div>
                </div>
              ) : (
                <div className="px-5 pb-4 space-y-2">
                  {cartItems.slice(0, 3).map((item) => (
                    <div key={item.cart_item_id} className="flex items-center justify-between text-xs">
                      <div className="flex-1 min-w-0">
                        <div className="font-medium text-mck-navy truncate">{item.description || `Item ${item.item_id}`}</div>
                        <div className="text-slate-500 mt-0.5">Qty {item.quantity} &middot; {formatCurrency(item.unit_price_at_add)}</div>
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
              )}
            </Card>

            <Card padding="none">
              <div className="px-5 pt-5 pb-3 flex items-center justify-between">
                <h3 className="text-sm font-semibold text-mck-navy">Recent purchases</h3>
                <button
                  type="button"
                  onClick={() => navigate('/customer/orders')}
                  className="text-xs font-semibold text-mck-blue hover:text-mck-blue-dark inline-flex items-center gap-1"
                >
                  All orders <ArrowRight size={12} />
                </button>
              </div>
              {historyQuery.isLoading ? (
                <div className="px-5 pb-5"><FullPanelSpinner label="Loading" /></div>
              ) : recentLines.length === 0 ? (
                <div className="px-5 pb-5 text-center text-xs text-slate-500">
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
        </div>
      </div>
    </AppShell>
  );
}
