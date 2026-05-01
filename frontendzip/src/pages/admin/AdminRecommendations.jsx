import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { ShoppingCart, CheckCircle2, TrendingUp, DollarSign, ArrowUpDown } from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card, { CardHeader } from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import HorizontalBarChart from '../../components/charts/HorizontalBarChart.jsx';
import RecentSalesFeed from '../../components/sales/RecentSalesFeed.jsx';
import SignalBadge from '../../components/ui/SignalBadge.jsx';
import { getConversionBySignal } from '../../api.js';
import { getSignal } from '../../lib/signals.js';
import { formatCurrency, formatNumber, formatPercentValue } from '../../lib/format.js';

// Admin recommendations

const SORT_OPTIONS = [
  { value: 'conversion_rate_pct', label: 'Conversion %' },
  { value: 'revenue_generated', label: 'Revenue' },
  { value: 'cart_adds', label: 'Cart adds' },
  { value: 'checkouts', label: 'Checkouts' }
];

export default function AdminRecommendations() {
  const [sortBy, setSortBy] = useState('conversion_rate_pct');

  const { data, isLoading, isError } = useQuery({
    queryKey: ['admin', 'conversion-by-signal'],
    queryFn: getConversionBySignal
  });

  const rows = useMemo(() => (data && data.rows) || [], [data]);

  // Sort rows by the selected metric (descending)
  const sortedRows = useMemo(() => {
    const numeric = (v) => {
      if (v === null || v === undefined) return 0;
      const n = typeof v === 'string' ? parseFloat(v) : v;
      return Number.isFinite(n) ? n : 0;
    };
    return [...rows].sort((a, b) => numeric(b[sortBy]) - numeric(a[sortBy]));
  }, [rows, sortBy]);

  return (
    <AppShell title="Recommendations" subtitle="Is the engine actually moving sales?">
      <div className="space-y-6">
        {/* Funnel KPIs */}
        <FunnelStrip data={data} loading={isLoading} />

        {/* Conversion-by-signal chart */}
        <Card>
          <CardHeader
            title="Conversion rate by signal"
            subtitle="Cart adds that became sales, broken out by which signal sourced the recommendation"
          />
          {isLoading ? (
            <div className="h-72 bg-slate-50 rounded animate-pulse" />
          ) : isError ? (
            <EmptyState title="Could not load conversion data" />
          ) : sortedRows.length === 0 ? (
            <EmptyState title="No signal data yet" description="Once recommendations drive cart activity, conversion rates will appear here." />
          ) : (
            <ConversionChart rows={sortedRows} />
          )}
        </Card>

        {/* Leaderboard table */}
        <Card>
          <CardHeader
            title="Signal leaderboard"
            subtitle="Drill into each signal's contribution"
            action={
              <SortMenu value={sortBy} onChange={setSortBy} />
            }
          />
          {isLoading ? (
            <FullPanelSpinner label="Loading signal leaderboard" />
          ) : isError ? (
            <EmptyState title="Could not load leaderboard" />
          ) : sortedRows.length === 0 ? (
            <EmptyState title="No signals to show" />
          ) : (
            <LeaderboardTable rows={sortedRows} />
          )}
        </Card>

        {/* Live sales feed */}
        <RecentSalesFeed limit={25} />
      </div>
    </AppShell>
  );
}

function FunnelStrip({ data, loading }) {
  if (loading) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className="h-24 bg-white border border-slate-200 rounded-lg animate-pulse" />
        ))}
      </div>
    );
  }
  if (!data) return <EmptyState title="No conversion data available" />;

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <StatCard
        label="Total cart adds"
        value={formatNumber(data.total_cart_adds)}
        icon={ShoppingCart}
        accent="mck-blue"
      />
      <StatCard
        label="Total checkouts"
        value={formatNumber(data.total_checkouts)}
        icon={CheckCircle2}
        accent="green"
      />
      <StatCard
        label="Overall conversion"
        value={formatPercentValue(data.overall_conversion_rate_pct)}
        hint="Adds that became sales"
        icon={TrendingUp}
        accent="mck-orange"
      />
      <StatCard
        label="Total rec revenue"
        value={formatCurrency(data.total_revenue)}
        icon={DollarSign}
        accent="mck-navy"
      />
    </div>
  );
}

function ConversionChart({ rows }) {
  // Build bars sorted by conversion %; use the SignalBadge color palette
  // so the bar chart visually matches the badges shown elsewhere in the app.
  const sorted = [...rows].sort((a, b) => (b.conversion_rate_pct || 0) - (a.conversion_rate_pct || 0));

  const bars = sorted.map((r) => {
    const code = r.source && r.source.code;
    const display = (r.source && r.source.display_name) || code || 'Unknown';
    return {
      label: display,
      value: r.conversion_rate_pct || 0,
      secondary: `${formatNumber(r.checkouts)} checkouts of ${formatNumber(r.cart_adds)} adds`
    };
  });

  // Map signal codes to their hex color for per-bar coloring
  const perRowColors = sorted.map((r) => {
    const code = r.source && r.source.code;
    return signalHex(code);
  });

  return (
    <HorizontalBarChart
      bars={bars}
      valueLabel="Conversion"
      formatValue={(v) => `${(v || 0).toFixed(1)}%`}
      perRowColors={perRowColors}
      height={Math.max(220, bars.length * 36)}
    />
  );
}

function LeaderboardTable({ rows }) {
  return (
    <div className="overflow-x-auto -mx-5">
      <table className="w-full text-sm">
        <thead>
          <tr className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">
            <th className="text-left px-5 py-2">Signal</th>
            <th className="text-right px-3 py-2">Cart adds</th>
            <th className="text-right px-3 py-2">Checkouts</th>
            <th className="text-right px-3 py-2">Abandons</th>
            <th className="text-right px-3 py-2">Conversion</th>
            <th className="text-right px-3 py-2">Quantity sold</th>
            <th className="text-right px-5 py-2">Revenue</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => {
            const code = row.source && row.source.code;
            return (
              <tr key={`${code}-${i}`} className="border-t border-slate-100 hover:bg-slate-50/50">
                <td className="px-5 py-2.5">
                  <SignalBadge signal={code} />
                </td>
                <td className="px-3 py-2.5 text-right text-mck-navy font-medium">{formatNumber(row.cart_adds)}</td>
                <td className="px-3 py-2.5 text-right text-mck-navy font-medium">{formatNumber(row.checkouts)}</td>
                <td className="px-3 py-2.5 text-right text-slate-500">{formatNumber(row.abandons)}</td>
                <td className="px-3 py-2.5 text-right">
                  <ConversionPill pct={row.conversion_rate_pct} />
                </td>
                <td className="px-3 py-2.5 text-right text-slate-600">{formatNumber(row.quantity_sold)}</td>
                <td className="px-5 py-2.5 text-right text-mck-navy font-semibold">{formatCurrency(row.revenue_generated)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ConversionPill({ pct }) {
  const v = pct || 0;
  let cls = 'text-slate-700 bg-slate-100';
  if (v >= 30) cls = 'text-green-700 bg-green-50';
  else if (v >= 15) cls = 'text-amber-700 bg-amber-50';
  else if (v > 0) cls = 'text-orange-700 bg-orange-50';
  return (
    <span className={`inline-block text-xs font-semibold px-2 py-0.5 rounded ${cls}`}>
      {formatPercentValue(v)}
    </span>
  );
}

function SortMenu({ value, onChange }) {
  return (
    <div className="flex items-center gap-2">
      <span className="text-xs text-slate-500 inline-flex items-center gap-1">
        <ArrowUpDown size={12} />
        Sort by
      </span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="text-xs bg-white border border-slate-200 rounded px-2 py-1 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue"
      >
        {SORT_OPTIONS.map((o) => (
          <option key={o.value} value={o.value}>
            {o.label}
          </option>
        ))}
      </select>
    </div>
  );
}

function signalHex(code) {
  // Pull the matching tailwind hex from the signal registry's dot color.
  // We can't introspect the dot class, so map known codes to hex directly.
  const map = {
    peer_gap: '#1B6BBE',
    popularity: '#0EA5E9',
    cart_complement: '#7C3AED',
    item_similarity: '#0F766E',
    replenishment: '#16A34A',
    lapsed_recovery: '#EA580C',
    private_brand_upgrade: '#DC2626',
    medline_conversion: '#9333EA',
    cart_complement_high_lift: '#7C3AED',
    private_brand_substitute: '#DC2626',
    medline_to_mckesson: '#9333EA',
    replenishment_due: '#16A34A',
    peer_gap_complement: '#1B6BBE'
  };
  return map[code] || '#1B6BBE';
}
