import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  Package,
  TrendingUp,
  ShoppingBag,
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  ChevronDown,
  ChevronUp
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card, { CardHeader } from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import SegmentedControl from '../../components/ui/SegmentedControl.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import LineChart from '../../components/charts/LineChart.jsx';
import ProductImage from '../../components/ui/ProductImage.jsx';
import { getSalesTrend, getRecentSales } from '../../api.js';
import { formatCurrency, formatNumber, formatDate } from '../../lib/format.js';

// Admin products

const RANGE_OPTIONS = [
  { value: '7d', label: '7d' },
  { value: '30d', label: '30d' },
  { value: '90d', label: '90d' },
  { value: '180d', label: '180d' },
  { value: '1y', label: '1y' },
  { value: 'all', label: 'All' }
];

const GRANULARITY_OPTIONS = [
  { value: 'daily', label: 'Daily' },
  { value: 'weekly', label: 'Weekly' },
  { value: 'monthly', label: 'Monthly' }
];

const SORT_OPTIONS = [
  { value: 'revenue', label: 'Revenue' },
  { value: 'units', label: 'Units sold' },
  { value: 'transactions', label: 'Transactions' }
];

const DIRECTION_OPTIONS = [
  { value: 'desc', label: 'Highest' },
  { value: 'asc', label: 'Lowest' }
];

export default function AdminProducts() {
  const [range, setRange] = useState('90d');
  const [granularity, setGranularity] = useState('daily');
  const [sortBy, setSortBy] = useState('revenue');
  const [direction, setDirection] = useState('desc');
  const [expanded, setExpanded] = useState(0);

  const trendQuery = useQuery({
    queryKey: ['admin', 'sales-trend', range, granularity],
    queryFn: () => getSalesTrend({ range, granularity })
  });

  // For product leaderboard, fetch up to 200 recent sales and aggregate client-side
  const recentQuery = useQuery({
    queryKey: ['admin', 'recent-sales', 200],
    queryFn: () => getRecentSales({ limit: 200 })
  });

  const aggregated = useMemo(() => {
    const rows = (recentQuery.data && recentQuery.data.rows) || [];
    const byItem = new Map();
    rows.forEach((r) => {
      const id = r.item_id;
      if (!byItem.has(id)) {
        byItem.set(id, {
          item_id: id,
          description: r.item_description || `Item ${id}`,
          family: r.family || '-',
          category: r.category || '-',
          units: 0,
          revenue: 0,
          transactions: 0,
          last_sold: r.sold_at,
          rec_driven: 0
        });
      }
      const agg = byItem.get(id);
      agg.units += r.quantity || 0;
      agg.revenue += parseFloat(r.line_total || 0);
      agg.transactions += 1;
      if (r.from_recommendation === true || r.from_recommendation === 1) agg.rec_driven += 1;
      if (r.sold_at && (!agg.last_sold || new Date(r.sold_at) > new Date(agg.last_sold))) {
        agg.last_sold = r.sold_at;
      }
    });
    return Array.from(byItem.values());
  }, [recentQuery.data]);

  const sorted = useMemo(() => {
    const arr = [...aggregated];
    arr.sort((a, b) => {
      const av = a[sortBy] || 0;
      const bv = b[sortBy] || 0;
      return direction === 'desc' ? bv - av : av - bv;
    });
    return arr;
  }, [aggregated, sortBy, direction]);

  const INITIAL = 15;
  const visible = expanded === 1 ? sorted : sorted.slice(0, INITIAL);
  const hidden = sorted.length - INITIAL;

  // Trend KPIs
  const trendBuckets = useMemo(() => {
    const buckets = (trendQuery.data && trendQuery.data.buckets) || [];
    return buckets.map((b) => ({ label: b.bucket, value: parseFloat(b.revenue || 0), order_count: b.order_count, quantity: b.quantity }));
  }, [trendQuery.data]);

  const totalRevenue = trendBuckets.reduce((acc, b) => acc + b.value, 0);
  const totalOrders = trendBuckets.reduce((acc, b) => acc + (b.order_count || 0), 0);
  const totalUnits = trendBuckets.reduce((acc, b) => acc + (b.quantity || 0), 0);

  return (
    <AppShell title="Products" subtitle="Sales trend and per-product performance">
      <div className="space-y-6">
        {/* Trend KPIs */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard label="Revenue" value={formatCurrency(totalRevenue)} hint={`${formatNumber(totalOrders)} orders`} icon={ShoppingBag} accent="green" />
          <StatCard label="Units sold" value={formatNumber(totalUnits)} icon={Package} accent="mck-blue" />
          <StatCard label="Distinct products" value={formatNumber(aggregated.length)} hint="in recent sales window" icon={TrendingUp} accent="mck-orange" />
          <StatCard label="Range" value={trendQuery.data ? `${trendQuery.data.range_start || ''} to ${trendQuery.data.range_end || ''}` : '-'} hint={trendQuery.data ? trendQuery.data.range_label : ''} icon={Package} accent="mck-navy" />
        </div>

        {/* Trend chart */}
        <Card>
          <CardHeader
            title="Sales trend"
            subtitle={trendQuery.data ? `${formatDate(trendQuery.data.range_start)} to ${formatDate(trendQuery.data.range_end)}` : ''}
            action={
              <div className="flex items-center gap-2 flex-wrap">
                <SegmentedControl options={GRANULARITY_OPTIONS} value={granularity} onChange={setGranularity} />
                <SegmentedControl options={RANGE_OPTIONS} value={range} onChange={setRange} />
              </div>
            }
          />
          {trendQuery.isLoading ? (
            <div className="h-72 bg-slate-50 rounded animate-pulse" />
          ) : trendQuery.isError ? (
            <EmptyState title="Could not load sales trend" />
          ) : (
            <LineChart
              buckets={trendBuckets}
              valueLabel="Revenue"
              formatValue={(v) => compactCurrency(v)}
              height={300}
            />
          )}
        </Card>

        {/* Product leaderboard */}
        <Card padding="none">
          <div className="px-5 pt-5 pb-3 flex items-start justify-between gap-3 flex-wrap">
            <div>
              <h3 className="text-sm font-semibold text-mck-navy">Product performance</h3>
              <p className="text-xs text-slate-500 mt-0.5">
                {sorted.length} products from the last 200 platform sales
                {expanded === 0 && sorted.length > INITIAL ? <> &middot; showing top {INITIAL}</> : null}
              </p>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-xs text-slate-500 inline-flex items-center gap-1">
                <ArrowUpDown size={11} />
                Sort by
              </span>
              <SegmentedControl options={SORT_OPTIONS} value={sortBy} onChange={setSortBy} />
              <SegmentedControl options={DIRECTION_OPTIONS} value={direction} onChange={setDirection} />
            </div>
          </div>

          {recentQuery.isLoading ? (
            <FullPanelSpinner label="Loading product data" />
          ) : recentQuery.isError ? (
            <div className="px-5 pb-6">
              <EmptyState title="Could not load product data" />
            </div>
          ) : sorted.length === 0 ? (
            <div className="px-5 pb-6">
              <EmptyState
                icon={Package}
                title="No product activity yet"
                description="Once sales are recorded, per-product aggregates will appear here."
              />
            </div>
          ) : (
            <div className="border-t border-slate-100">
              <table className="w-full text-sm">
                <thead>
                  <tr className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">
                    <th className="text-left px-5 py-2">#</th>
                    <th className="text-left px-3 py-2">Product</th>
                    <th className="text-left px-3 py-2">Family</th>
                    <th className="text-right px-3 py-2">Units</th>
                    <th className="text-right px-3 py-2">Txns</th>
                    <th className="text-right px-3 py-2">Rec-driven</th>
                    <th className="text-right px-5 py-2">Revenue</th>
                  </tr>
                </thead>
                <tbody>
                  {visible.map((p, i) => (
                    <tr key={p.item_id} className="border-t border-slate-100 hover:bg-slate-50/40">
                      <td className="px-5 py-2 text-slate-400 text-xs">{direction === 'desc' ? i + 1 : sorted.length - i}</td>
                      <td className="px-3 py-2">
                        <div className="flex items-center gap-2">
                          <ProductImage size="xs" alt={p.description} />
                          <div className="min-w-0">
                            <div className="text-mck-navy font-semibold truncate max-w-[18rem]">{p.description}</div>
                            <div className="text-[10px] text-slate-400">SKU {p.item_id} &middot; {p.category}</div>
                          </div>
                        </div>
                      </td>
                      <td className="px-3 py-2 text-slate-600 truncate max-w-[12rem]">{p.family}</td>
                      <td className="px-3 py-2 text-right text-slate-700">{formatNumber(p.units)}</td>
                      <td className="px-3 py-2 text-right text-slate-700">{formatNumber(p.transactions)}</td>
                      <td className="px-3 py-2 text-right">
                        {p.rec_driven > 0 ? (
                          <span className="inline-flex items-center text-[10px] font-semibold text-mck-orange bg-orange-50 px-1.5 py-0.5 rounded">
                            {p.rec_driven}
                          </span>
                        ) : (
                          <span className="text-slate-300">-</span>
                        )}
                      </td>
                      <td className="px-5 py-2 text-right text-mck-navy font-semibold">{formatCurrency(p.revenue)}</td>
                    </tr>
                  ))}
                </tbody>
                {sorted.length > INITIAL ? (
                  <tfoot>
                    <tr className="border-t border-slate-100">
                      <td colSpan={7} className="p-0">
                        <button
                          type="button"
                          onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
                          className="w-full px-5 py-2.5 text-xs font-semibold text-mck-blue hover:text-mck-blue-dark hover:bg-mck-sky/40 flex items-center justify-center gap-1.5"
                        >
                          {expanded === 1 ? (
                            <>
                              <ChevronUp size={14} />
                              Show less
                            </>
                          ) : (
                            <>
                              <ChevronDown size={14} />
                              Show all {sorted.length} products ({hidden} more)
                            </>
                          )}
                        </button>
                      </td>
                    </tr>
                  </tfoot>
                ) : null}
              </table>
            </div>
          )}
        </Card>
      </div>
    </AppShell>
  );
}

function compactCurrency(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
}
