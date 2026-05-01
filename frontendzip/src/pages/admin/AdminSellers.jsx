import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import {
  Briefcase,
  CheckCircle2,
  TrendingUp,
  DollarSign,
  Search,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Crown
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card, { CardHeader } from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import SegmentedControl from '../../components/ui/SegmentedControl.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import HorizontalBarChart from '../../components/charts/HorizontalBarChart.jsx';
import { getTopSellers, listUsers } from '../../api.js';
import { formatCurrency, formatNumber, relativeTime } from '../../lib/format.js';

// Admin sellers

const RANGE_OPTIONS = [
  { value: '7d', label: '7d' },
  { value: '30d', label: '30d' },
  { value: '90d', label: '90d' },
  { value: '180d', label: '180d' },
  { value: '1y', label: '1y' },
  { value: 'all', label: 'All' }
];

const STATUS_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'active', label: 'Active' },
  { value: 'inactive', label: 'Inactive' }
];

export default function AdminSellers() {
  const [range, setRange] = useState('all');
  const [statusFilter, setStatusFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [rosterExpanded, setRosterExpanded] = useState(0);

  const navigate = useNavigate();

  const topSellersQuery = useQuery({
    queryKey: ['admin', 'top-sellers', range],
    queryFn: () => getTopSellers({ range, limit: 50 })
  });

  const rosterQuery = useQuery({
    queryKey: ['admin', 'sellers-roster'],
    queryFn: () => listUsers({ role: 'seller', limit: 500, offset: 0 })
  });

  const topSellerRows = useMemo(() => (topSellersQuery.data && topSellersQuery.data.rows) || [], [topSellersQuery.data]);
  const roster = useMemo(() => (rosterQuery.data && rosterQuery.data.items) || [], [rosterQuery.data]);

  // KPI: total + active + top revenue + top AOV
  const kpis = useMemo(() => {
    const total = roster.length;
    const active = roster.filter((u) => u.is_active === true || u.is_active === 1).length;
    let topRev = 0;
    let topName = '-';
    let topAov = 0;
    let topAovName = '-';
    topSellerRows.forEach((r) => {
      const rev = parseFloat(r.total_revenue || 0);
      const aov = parseFloat(r.avg_order_value || 0);
      if (rev > topRev) {
        topRev = rev;
        topName = r.seller_full_name || r.seller_username || '-';
      }
      if (aov > topAov) {
        topAov = aov;
        topAovName = r.seller_full_name || r.seller_username || '-';
      }
    });
    return { total, active, topRev, topName, topAov, topAovName };
  }, [roster, topSellerRows]);

  // Filtered roster
  const filteredRoster = useMemo(() => {
    const q = search.trim().toLowerCase();
    return roster.filter((u) => {
      const isActive = u.is_active === true || u.is_active === 1;
      if (statusFilter === 'active' && !isActive) return 0;
      if (statusFilter === 'inactive' && isActive) return 0;
      if (!q) return 1;
      const hay = `${u.username || ''} ${u.full_name || ''} ${u.email || ''}`.toLowerCase();
      return hay.indexOf(q) !== -1 ? 1 : 0;
    });
  }, [roster, search, statusFilter]);

  const ROSTER_INITIAL = 10;
  const visibleRoster = rosterExpanded === 1 ? filteredRoster : filteredRoster.slice(0, ROSTER_INITIAL);
  const hiddenRoster = filteredRoster.length - ROSTER_INITIAL;

  return (
    <AppShell title="Sellers" subtitle="Performance and roster">
      <div className="space-y-6">
        {/* KPI strip */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <StatCard
            label="Total sellers"
            value={formatNumber(kpis.total)}
            icon={Briefcase}
            accent="mck-navy"
          />
          <StatCard
            label="Active"
            value={formatNumber(kpis.active)}
            hint={`${kpis.total - kpis.active} inactive`}
            icon={CheckCircle2}
            accent="green"
          />
          <StatCard
            label="Top revenue"
            value={formatCurrency(kpis.topRev)}
            hint={kpis.topName}
            icon={DollarSign}
            accent="mck-orange"
          />
          <StatCard
            label="Top avg order"
            value={formatCurrency(kpis.topAov)}
            hint={kpis.topAovName}
            icon={TrendingUp}
            accent="mck-blue"
          />
        </div>

        {/* Top sellers bar chart */}
        <Card>
          <CardHeader
            title="Top sellers by revenue"
            subtitle={topSellerSubtitle(topSellersQuery.data)}
            action={<SegmentedControl options={RANGE_OPTIONS} value={range} onChange={setRange} />}
          />
          {topSellersQuery.isLoading ? (
            <div className="h-72 bg-slate-50 rounded animate-pulse" />
          ) : topSellersQuery.isError ? (
            <EmptyState title="Could not load top sellers" />
          ) : topSellerRows.length === 0 ? (
            <EmptyState title="No sales activity in this range" description="Adjust the time range or check that sales data has been recorded." />
          ) : (
            <TopSellersChart rows={topSellerRows} onClickRow={(seller) => navigate(`/admin/sellers/${seller.seller_id}`)} />
          )}
        </Card>

        {/* Roster table */}
        <Card padding="none">
          <div className="px-5 pt-5 pb-3 flex items-start justify-between gap-3 flex-wrap">
            <div>
              <h2 className="text-base font-semibold text-mck-navy">All sellers</h2>
              <p className="text-xs text-slate-500 mt-0.5">
                {filteredRoster.length} of {roster.length} sellers
                {rosterExpanded === 0 && filteredRoster.length > ROSTER_INITIAL ? <> &middot; showing top {ROSTER_INITIAL}</> : null}
              </p>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <div className="relative">
                <Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
                <input
                  type="text"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search by name, username, email"
                  className="pl-8 pr-3 py-1.5 text-xs border border-slate-200 rounded-md focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue placeholder:text-slate-400 w-56"
                />
              </div>
              <SegmentedControl options={STATUS_OPTIONS} value={statusFilter} onChange={setStatusFilter} />
            </div>
          </div>

          {rosterQuery.isLoading ? (
            <FullPanelSpinner label="Loading roster" />
          ) : rosterQuery.isError ? (
            <div className="px-5 pb-6">
              <EmptyState title="Could not load seller roster" />
            </div>
          ) : filteredRoster.length === 0 ? (
            <div className="px-5 pb-6">
              <EmptyState
                icon={Briefcase}
                title={roster.length === 0 ? 'No sellers yet' : 'No sellers match the filter'}
                description={roster.length === 0 ? 'Create a seller account to get started.' : 'Try clearing the search or status filter.'}
              />
            </div>
          ) : (
            <div className="border-t border-slate-100">
              <div className="divide-y divide-slate-100">
                {visibleRoster.map((u) => (
                  <RosterRow
                    key={u.user_id}
                    user={u}
                    perfRow={topSellerRows.find((r) => r.seller_id === u.user_id)}
                    onClick={() => navigate(`/admin/sellers/${u.user_id}`)}
                  />
                ))}
              </div>
              {filteredRoster.length > ROSTER_INITIAL ? (
                <div className="border-t border-slate-100">
                  <button
                    type="button"
                    onClick={() => setRosterExpanded(rosterExpanded === 1 ? 0 : 1)}
                    className="w-full px-5 py-2.5 text-xs font-semibold text-mck-blue hover:text-mck-blue-dark hover:bg-mck-sky/40 transition-colors flex items-center justify-center gap-1.5"
                  >
                    {rosterExpanded === 1 ? (
                      <>
                        <ChevronUp size={14} />
                        Show less
                      </>
                    ) : (
                      <>
                        <ChevronDown size={14} />
                        Show all {filteredRoster.length} sellers ({hiddenRoster} more)
                      </>
                    )}
                  </button>
                </div>
              ) : null}
            </div>
          )}
        </Card>
      </div>
    </AppShell>
  );
}

function TopSellersChart({ rows, onClickRow }) {
  // Sort by revenue desc, take top 20 for the chart, but keep original full list searchable below
  const sorted = [...rows].sort((a, b) => parseFloat(b.total_revenue || 0) - parseFloat(a.total_revenue || 0));
  const topN = sorted.slice(0, 20);

  const bars = topN.map((r) => ({
    label: r.seller_full_name || r.seller_username || `Seller ${r.seller_id}`,
    value: parseFloat(r.total_revenue || 0),
    secondary: `${formatNumber(r.total_sales)} sales · ${formatNumber(r.customers_managed)} customers · AOV ${formatCurrency(r.avg_order_value)}`
  }));

  const handleClick = (idx) => {
    const seller = topN[idx];
    if (seller && onClickRow) onClickRow(seller);
  };

  return (
    <div>
      <HorizontalBarChart
        bars={bars}
        valueLabel="Revenue"
        formatValue={(v) => compactCurrency(v)}
        height={Math.max(220, bars.length * 32)}
      />
      {/* Click hints below the chart */}
      <div className="mt-3 text-[11px] text-slate-500 flex items-center gap-1">
        <Crown size={11} className="text-mck-orange" />
        <span>Top {bars.length} sellers shown. Click any seller in the roster below to see their full performance.</span>
      </div>
    </div>
  );
}

function RosterRow({ user, perfRow, onClick }) {
  const isActive = user.is_active === true || user.is_active === 1;
  const customers = perfRow ? formatNumber(perfRow.customers_managed) : '-';
  const revenue = perfRow ? formatCurrency(perfRow.total_revenue) : '-';

  return (
    <button
      type="button"
      onClick={onClick}
      className="w-full text-left px-5 py-3 hover:bg-mck-sky/30 transition-colors flex items-center gap-4"
    >
      <div className="flex-shrink-0 w-9 h-9 rounded-full bg-mck-sky text-mck-blue font-semibold text-xs flex items-center justify-center">
        {initials(user)}
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-semibold text-mck-navy truncate">
            {user.full_name || user.username}
          </span>
          <span className="text-[10px] uppercase tracking-wider text-slate-400">@{user.username}</span>
          {isActive ? (
            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-green-700 bg-green-50 border border-green-200 px-1.5 py-0.5 rounded">
              <span className="w-1 h-1 rounded-full bg-green-500" />
              Active
            </span>
          ) : (
            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-slate-500 bg-slate-100 border border-slate-200 px-1.5 py-0.5 rounded">
              Inactive
            </span>
          )}
        </div>
        <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-x-2 gap-y-0.5 flex-wrap">
          {user.email ? <span className="truncate">{user.email}</span> : null}
          <span className="text-slate-300">|</span>
          <span>Last login {user.last_login_at ? relativeTime(user.last_login_at) : 'never'}</span>
        </div>
      </div>

      <div className="flex-shrink-0 text-right hidden md:block">
        <div className="text-xs font-semibold text-mck-navy">{revenue}</div>
        <div className="text-[10px] text-slate-500">{customers} customers</div>
      </div>

      <ChevronRight size={16} className="text-slate-400 flex-shrink-0" />
    </button>
  );
}

function topSellerSubtitle(data) {
  if (!data) return '';
  const start = data.range_start ? new Date(data.range_start).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '';
  const end = data.range_end ? new Date(data.range_end).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' }) : '';
  if (!start || !end) return data.range_label || '';
  return `${start} to ${end}`;
}

function compactCurrency(n) {
  if (n === null || n === undefined || Number.isNaN(n)) return '-';
  const abs = Math.abs(n);
  if (abs >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (abs >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
}

function initials(user) {
  if (!user) return '?';
  if (user.full_name) {
    const parts = user.full_name.trim().split(/\s+/);
    if (parts.length >= 2) return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    return user.full_name.slice(0, 2).toUpperCase();
  }
  return (user.username || '?').slice(0, 2).toUpperCase();
}
