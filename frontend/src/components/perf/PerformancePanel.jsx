import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import {
  ShoppingBag,
  DollarSign,
  Package,
  Repeat,
  Calendar,
  ChevronDown,
  ChevronUp
} from 'lucide-react';
import Card, { CardHeader } from '../ui/Card.jsx';
import StatCard from '../ui/StatCard.jsx';
import SegmentedControl from '../ui/SegmentedControl.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import LineChart from '../charts/LineChart.jsx';
import { formatCurrency, formatNumber, formatDate } from '../../lib/format.js';

// Performance panel

// Reusable performance dashboard. Accepts a `fetch(params)` function
// returning a payload shaped like /sellers/me/stats or
// /customers/{id}/stats:
//   { summary: {...}, trend: [...], top_products: [...], top_families: [...], range_label, has_data }
// Used on the seller's performance page, the customer self-service view,
// and on the admin's customer detail page.

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

export default function PerformancePanel({ queryKey, fetcher, title = 'Performance', subtitle, defaultRange = '90d' }) {
  const [range, setRange] = useState(defaultRange);
  const [granularity, setGranularity] = useState('daily');

  const { data, isLoading, isError } = useQuery({
    queryKey: [...queryKey, range, granularity],
    queryFn: () => fetcher({ range, granularity, top_products: 10, top_families: 5 })
  });

  if (isLoading) return <FullPanelSpinner label="Loading performance" />;
  if (isError || !data) {
    return <EmptyState title="Could not load performance data" description="Please try again." />;
  }

  const summary = data.summary || {};
  const hasData = data.has_data === true || data.has_data === 1;

  return (
    <div className="space-y-6">
      <div className="flex items-end justify-between flex-wrap gap-3">
        <div>
          <h2 className="text-base font-semibold text-mck-navy">{title}</h2>
          {subtitle ? <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p> : null}
        </div>
        <div className="flex items-center gap-2 flex-wrap">
          <SegmentedControl options={GRANULARITY_OPTIONS} value={granularity} onChange={setGranularity} />
          <SegmentedControl options={RANGE_OPTIONS} value={range} onChange={setRange} />
        </div>
      </div>

      {!hasData ? (
        <EmptyState
          title="No activity in this range"
          description="There are no purchases on record for the selected window. Try a wider range."
        />
      ) : (
        <>
          {/* KPI strip */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <StatCard label="Total revenue" value={formatCurrency(summary.total_revenue)} icon={DollarSign} accent="green" />
            <StatCard label="Total orders" value={formatNumber(summary.total_orders)} icon={ShoppingBag} accent="mck-blue" />
            <StatCard label="Avg order value" value={formatCurrency(summary.avg_order_value)} icon={Repeat} accent="mck-orange" />
            <StatCard label="Distinct products" value={formatNumber(summary.distinct_products_purchased)} hint={`${formatNumber(summary.total_items_purchased)} units`} icon={Package} accent="mck-navy" />
          </div>

          <div className="text-xs text-slate-500 inline-flex items-center gap-1.5">
            <Calendar size={12} />
            {formatDate(summary.first_order_date)} to {formatDate(summary.last_order_date)}
          </div>

          {/* Revenue trend */}
          <Card>
            <CardHeader title="Revenue trend" subtitle={data.range_label} />
            <TrendChart trend={data.trend || []} />
          </Card>

          {/* Top products + Top families side by side */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card padding="none">
              <div className="px-5 pt-5 pb-3">
                <h3 className="text-sm font-semibold text-mck-navy">Top products</h3>
                <p className="text-xs text-slate-500 mt-0.5">Highest-grossing items in this window</p>
              </div>
              <ProductsTable rows={data.top_products || []} />
            </Card>
            <Card padding="none">
              <div className="px-5 pt-5 pb-3">
                <h3 className="text-sm font-semibold text-mck-navy">Top families</h3>
                <p className="text-xs text-slate-500 mt-0.5">Highest-grossing product families</p>
              </div>
              <FamiliesTable rows={data.top_families || []} />
            </Card>
          </div>
        </>
      )}
    </div>
  );
}

function TrendChart({ trend }) {
  const buckets = trend.map((b) => ({
    label: b.bucket,
    value: parseFloat(b.revenue || 0)
  }));
  return (
    <LineChart
      buckets={buckets}
      valueLabel="Revenue"
      formatValue={(v) => compactCurrency(v)}
      height={280}
    />
  );
}

function ProductsTable({ rows }) {
  const [expanded, setExpanded] = useState(0);
  const INITIAL = 10;
  if (!rows || rows.length === 0) {
    return (
      <div className="px-5 pb-6">
        <EmptyState title="No products" description="No product activity for the selected window." />
      </div>
    );
  }
  const visible = expanded === 1 ? rows : rows.slice(0, INITIAL);
  return (
    <div className="border-t border-slate-100">
      <table className="w-full text-sm table-fixed">
        <colgroup>
          <col className="w-12" />
          <col />
          <col className="w-20" />
          <col className="w-32" />
        </colgroup>
        <thead>
          <tr className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">
            <th className="text-left px-5 py-2">#</th>
            <th className="text-left px-3 py-2">Product</th>
            <th className="text-right px-3 py-2">Qty</th>
            <th className="text-right px-5 py-2">Revenue</th>
          </tr>
        </thead>
        <tbody>
          {visible.map((p, i) => (
            <tr key={p.item_id || i} className="border-t border-slate-100 hover:bg-slate-50/50">
              <td className="px-5 py-2 text-slate-400 text-xs align-top">{i + 1}</td>
              <td className="px-3 py-2 min-w-0">
                <div
                  className="text-mck-navy font-medium truncate"
                  title={p.description || `Item ${p.item_id}`}
                >
                  {p.description || `Item ${p.item_id}`}
                </div>
                <div className="text-[11px] text-slate-500 truncate" title={p.family || ''}>
                  {p.family || ''}
                </div>
              </td>
              <td className="px-3 py-2 text-right text-slate-600 align-top">{formatNumber(p.quantity)}</td>
              <td className="px-5 py-2 text-right text-mck-navy font-semibold align-top">{formatCurrency(p.revenue)}</td>
            </tr>
          ))}
        </tbody>
        {rows.length > INITIAL ? (
          <tfoot>
            <tr className="border-t border-slate-100">
              <td colSpan={4} className="p-0">
                <button
                  type="button"
                  onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
                  className="w-full px-5 py-2.5 text-xs font-semibold text-mck-blue hover:text-mck-blue-dark hover:bg-mck-sky/40 flex items-center justify-center gap-1.5"
                >
                  {expanded === 1 ? <><ChevronUp size={14} />Show less</> : <><ChevronDown size={14} />Show all {rows.length} products</>}
                </button>
              </td>
            </tr>
          </tfoot>
        ) : null}
      </table>
    </div>
  );
}

function FamiliesTable({ rows }) {
  if (!rows || rows.length === 0) {
    return (
      <div className="px-5 pb-6">
        <EmptyState title="No families" description="No family activity for the selected window." />
      </div>
    );
  }
  return (
    <div className="border-t border-slate-100">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">
            <th className="text-left px-5 py-2">Family</th>
            <th className="text-right px-3 py-2">Orders</th>
            <th className="text-right px-5 py-2">Revenue</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((f, i) => (
            <tr key={f.family || i} className="border-t border-slate-100 hover:bg-slate-50/50">
              <td className="px-5 py-2 text-mck-navy font-medium truncate max-w-[20rem]">{f.family || '-'}</td>
              <td className="px-3 py-2 text-right text-slate-600">{formatNumber(f.order_count || f.orders)}</td>
              <td className="px-5 py-2 text-right text-mck-navy font-semibold">{formatCurrency(f.revenue)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function compactCurrency(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
}
