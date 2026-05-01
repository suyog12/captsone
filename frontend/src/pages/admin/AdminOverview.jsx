import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Building2, Users, ShoppingBag, Package, TrendingUp, Calendar, ChevronDown, ChevronUp } from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import TopCustomersPanel from '../../components/admin/TopCustomersPanel.jsx';
import EngineEffectivenessPanel from '../../components/admin/EngineEffectivenessPanel.jsx';
import ChurnFunnelCard from '../../components/admin/ChurnFunnelCard.jsx';
import Card, { CardHeader } from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import SegmentedControl from '../../components/ui/SegmentedControl.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import LineChart from '../../components/charts/LineChart.jsx';
import { getAdminOverview, getSalesTrend } from '../../api.js';
import { formatCurrency, formatNumber, formatDate } from '../../lib/format.js';

// Admin overview

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

export default function AdminOverview() {
  const [range, setRange] = useState('90d');
  const [granularity, setGranularity] = useState('daily');

  const overview = useQuery({
    queryKey: ['admin', 'overview'],
    queryFn: getAdminOverview
  });

  const trend = useQuery({
    queryKey: ['admin', 'sales-trend', range, granularity],
    queryFn: () => getSalesTrend({ range, granularity })
  });

  return (
    <AppShell title="Platform overview" subtitle="Population, products, sales activity, engine performance">
      <div className="space-y-6">
        <KpiStrip data={overview.data} loading={overview.isLoading} />

        {/* Customer lifecycle funnel - cold start / stable / declining / churned */}
        <ChurnFunnelCard />

        {/* Engine effectiveness - the recommendation engine ROI story */}
        <EngineEffectivenessPanel />

        <Card>
          <CardHeader
            title="Sales trend"
            subtitle={trendSubtitle(trend.data)}
            action={
              <div className="flex items-center gap-2 flex-wrap">
                <SegmentedControl options={GRANULARITY_OPTIONS} value={granularity} onChange={setGranularity} />
                <SegmentedControl options={RANGE_OPTIONS} value={range} onChange={setRange} />
              </div>
            }
          />
          <SalesTrendChart trend={trend} />
        </Card>

        <TopCustomersPanel />
      </div>
    </AppShell>
  );
}

function KpiStrip({ data, loading }) {
  if (loading) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className="h-24 bg-white border border-slate-200 rounded-lg animate-pulse" />
        ))}
      </div>
    );
  }
  if (!data) {
    return <EmptyState title="Overview unavailable" description="Could not load platform statistics." />;
  }

  const pop = data.customer_population || {};
  const prod = data.products || {};
  const last7 = data.sales_last_7_days || {};
  const last30 = data.sales_last_30_days || {};

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          label="Total customers"
          value={formatNumber(pop.total_customers)}
          hint={`${formatNumber(pop.active_accounts)} active`}
          icon={Building2}
          accent="mck-blue"
        />
        <StatCard
          label="New this month"
          value={formatNumber(pop.new_customers_this_month)}
          icon={Users}
          accent="mck-orange"
        />
        <StatCard
          label="Total products"
          value={formatNumber(prod.total_products)}
          hint={`${formatNumber(prod.private_brand_products)} private brand (${(prod.private_brand_pct || 0).toFixed(1)}%)`}
          icon={Package}
          accent="mck-navy"
        />
        <StatCard
          label="Revenue (30d)"
          value={formatCurrency(last30.revenue)}
          hint={`${formatNumber(last30.transactions)} transactions`}
          icon={ShoppingBag}
          accent="green"
        />
      </div>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <MiniStat label="Sales (7d)" value={formatCurrency(last7.revenue)} hint={`${formatNumber(last7.transactions)} txns`} />
        <MiniStat label="Sales (30d)" value={formatCurrency(last30.revenue)} hint={`${formatNumber(last30.transactions)} txns`} />
        <MiniStat
          label="Distinct customers (30d)"
          value={formatNumber(last30.distinct_customers)}
        />
        <MiniStat
          label="Distinct sellers (30d)"
          value={formatNumber(last30.distinct_sellers)}
        />
      </div>
    </div>
  );
}

function MiniStat({ label, value, hint }) {
  return (
    <div className="bg-white rounded-lg border border-slate-200 px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">{label}</div>
      <div className="text-base font-bold text-mck-navy mt-0.5">{value}</div>
      {hint ? <div className="text-[10px] text-slate-500 mt-0.5">{hint}</div> : null}
    </div>
  );
}

function trendSubtitle(data) {
  if (!data) return '';
  const start = formatDate(data.range_start);
  const end = formatDate(data.range_end);
  if (start === '-' || end === '-') return '';
  return `${start} to ${end}`;
}

function SalesTrendChart({ trend }) {
  if (trend.isLoading) return <div className="h-72 bg-slate-50 rounded animate-pulse" />;
  if (trend.isError || !trend.data) {
    return (
      <div className="flex items-center justify-center h-72 text-sm text-slate-400">
        Could not load sales trend
      </div>
    );
  }

  const buckets = (trend.data.buckets || []).map((b) => ({
    label: b.bucket,
    value: parseFloat(b.revenue || 0)
  }));

  return (
    <div>
      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mb-4">
        <TrendStat label="Total revenue" value={formatCurrency(sumValue(buckets))} />
        <TrendStat label="Total orders" value={formatNumber(sumOrderCount(trend.data.buckets))} />
        <TrendStat label="Buckets" value={formatNumber(trend.data.total_buckets)} />
        <TrendStat label="Granularity" value={capitalize(trend.data.granularity)} />
      </div>
      <LineChart
        buckets={buckets}
        valueLabel="Revenue"
        formatValue={(v) => compactCurrency(v)}
        height={300}
      />
    </div>
  );
}

function TrendStat({ label, value }) {
  return (
    <div className="bg-slate-50 border border-slate-100 rounded px-3 py-2">
      <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">{label}</div>
      <div className="text-sm font-bold text-mck-navy mt-0.5 truncate">{value}</div>
    </div>
  );
}

// Helpers

function sumValue(buckets) {
  return buckets.reduce((acc, b) => acc + (Number(b.value) || 0), 0);
}

function sumOrderCount(rawBuckets) {
  if (!rawBuckets) return 0;
  return rawBuckets.reduce((acc, b) => acc + (b.order_count || 0), 0);
}

function compactCurrency(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
}

function capitalize(s) {
  if (!s) return '';
  return s.charAt(0).toUpperCase() + s.slice(1);
}
