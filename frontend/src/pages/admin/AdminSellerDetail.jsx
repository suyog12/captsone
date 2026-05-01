import { useState, useMemo } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import {
  ArrowLeft,
  Briefcase,
  Users,
  DollarSign,
  ShoppingBag,
  TrendingUp,
  Mail,
  Calendar,
  Activity,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Package
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card, { CardHeader } from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import LifecycleBadge from '../../components/ui/LifecycleBadge.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import SignalBadge from '../../components/ui/SignalBadge.jsx';
import { getUser, getSellerCustomers, getTopSellers, getRecentSales } from '../../api.js';
import { formatCurrency, formatNumber, formatDate, relativeTime } from '../../lib/format.js';

// Admin seller detail

export default function AdminSellerDetail() {
  const { userId } = useParams();
  const navigate = useNavigate();
  const numericId = Number(userId);

  const userQuery = useQuery({
    queryKey: ['admin', 'user', numericId],
    queryFn: () => getUser(numericId),
    enabled: Number.isFinite(numericId)
  });

  const customersQuery = useQuery({
    queryKey: ['admin', 'seller', numericId, 'customers'],
    queryFn: () => getSellerCustomers(numericId, 500, 0),
    enabled: Number.isFinite(numericId)
  });

  const perfQuery = useQuery({
    queryKey: ['admin', 'top-sellers', 'all'],
    queryFn: () => getTopSellers({ range: 'all', limit: 50 })
  });

  // Fetch recent sales (largest practical limit) and filter to this seller client-side.
  const recentQuery = useQuery({
    queryKey: ['admin', 'recent-sales', 200],
    queryFn: () => getRecentSales({ limit: 200 })
  });

  const user = userQuery.data;
  const customers = (customersQuery.data && customersQuery.data.items) || [];
  const perfRow = useMemo(() => {
    const rows = (perfQuery.data && perfQuery.data.rows) || [];
    return rows.find((r) => r.seller_id === numericId);
  }, [perfQuery.data, numericId]);

  const sellerSales = useMemo(() => {
    const all = (recentQuery.data && recentQuery.data.rows) || [];
    return all.filter((row) => row.sold_by_seller_id === numericId);
  }, [recentQuery.data, numericId]);

  if (userQuery.isLoading) {
    return (
      <AppShell title="Loading seller">
        <FullPanelSpinner label="Loading seller profile" />
      </AppShell>
    );
  }

  if (userQuery.isError || !user || user.role !== 'seller') {
    return (
      <AppShell title="Seller not found">
        <EmptyState
          icon={Briefcase}
          title="Seller not found"
          description="This user could not be loaded or is not a seller account."
          action={
            <button
              type="button"
              onClick={() => navigate('/admin/sellers')}
              className="px-3 py-1.5 text-sm bg-mck-blue text-white rounded hover:bg-mck-blue-dark"
            >
              Back to sellers
            </button>
          }
        />
      </AppShell>
    );
  }

  return (
    <AppShell
      title={user.full_name || user.username}
      subtitle={`@${user.username}`}
      actions={
        <button
          type="button"
          onClick={() => navigate('/admin/sellers')}
          className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-600 hover:text-mck-navy px-2 py-1 rounded hover:bg-slate-100"
        >
          <ArrowLeft size={14} />
          Back
        </button>
      }
    >
      <div className="space-y-6">
        <SellerHeader user={user} />
        <PerformanceStrip perfRow={perfRow} customerCount={customers.length} loading={perfQuery.isLoading} />
        <PortfolioSection customers={customers} loading={customersQuery.isLoading} onClickCustomer={(c) => navigate(`/admin/customers/${c.cust_id}`)} />
        <RecentSalesBySeller sales={sellerSales} loading={recentQuery.isLoading} />
      </div>
    </AppShell>
  );
}

function SellerHeader({ user }) {
  const isActive = user.is_active === true || user.is_active === 1;
  return (
    <Card padding="lg">
      <div className="flex items-start gap-5 flex-wrap">
        <div className="flex-shrink-0 w-16 h-16 rounded-lg bg-mck-blue text-white font-bold text-xl flex items-center justify-center">
          {initials(user)}
        </div>

        <div className="flex-1 min-w-[18rem]">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-xl font-semibold text-mck-navy">
              {user.full_name || user.username}
            </h2>
            {isActive ? (
              <span className="inline-flex items-center gap-1 text-xs font-semibold text-green-700 bg-green-50 border border-green-200 px-2 py-0.5 rounded-full">
                <span className="w-1.5 h-1.5 rounded-full bg-green-500" />
                Active
              </span>
            ) : (
              <span className="inline-flex items-center gap-1 text-xs font-semibold text-slate-500 bg-slate-100 border border-slate-200 px-2 py-0.5 rounded-full">
                Inactive
              </span>
            )}
          </div>

          <div className="mt-2 grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-2 text-xs">
            <Field icon={Briefcase} label="User ID" value={`#${user.user_id}`} />
            <Field icon={Mail} label="Email" value={user.email || '-'} />
            <Field icon={Calendar} label="Created" value={formatDate(user.created_at)} />
            <Field icon={Activity} label="Last login" value={user.last_login_at ? relativeTime(user.last_login_at) : 'Never'} />
          </div>
        </div>
      </div>
    </Card>
  );
}

function PerformanceStrip({ perfRow, customerCount, loading }) {
  if (loading) {
    return (
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className="h-24 bg-white border border-slate-200 rounded-lg animate-pulse" />
        ))}
      </div>
    );
  }

  const revenue = perfRow ? formatCurrency(perfRow.total_revenue) : '$0.00';
  const sales = perfRow ? formatNumber(perfRow.total_sales) : '0';
  const aov = perfRow ? formatCurrency(perfRow.avg_order_value) : '$0.00';
  const qty = perfRow ? formatNumber(perfRow.total_quantity_sold) : '0';

  return (
    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
      <StatCard label="Customers managed" value={formatNumber(customerCount)} icon={Users} accent="mck-navy" />
      <StatCard label="Total revenue (all time)" value={revenue} icon={DollarSign} accent="green" />
      <StatCard label="Total sales" value={sales} hint={`${qty} units`} icon={ShoppingBag} accent="mck-blue" />
      <StatCard label="Avg order value" value={aov} icon={TrendingUp} accent="mck-orange" />
    </div>
  );
}

function PortfolioSection({ customers, loading, onClickCustomer }) {
  const [expanded, setExpanded] = useState(0);
  const INITIAL = 10;

  // Lifecycle counts for a quick visual breakdown
  const counts = useMemo(() => {
    const out = { stable_warm: 0, declining_warm: 0, churned_warm: 0, cold_start: 0 };
    customers.forEach((c) => {
      if (c.status && out[c.status] !== undefined) out[c.status] += 1;
    });
    return out;
  }, [customers]);

  if (loading) {
    return (
      <Card>
        <CardHeader title="Customer portfolio" subtitle="Customers assigned to this seller" />
        <FullPanelSpinner label="Loading portfolio" />
      </Card>
    );
  }

  const visible = expanded === 1 ? customers : customers.slice(0, INITIAL);
  const hidden = customers.length - INITIAL;

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3">
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div>
            <h2 className="text-base font-semibold text-mck-navy">Customer portfolio</h2>
            <p className="text-xs text-slate-500 mt-0.5">
              {customers.length} customers
              {expanded === 0 && customers.length > INITIAL ? <> &middot; showing top {INITIAL}</> : null}
            </p>
          </div>
          <div className="flex items-center gap-2 flex-wrap">
            <LifecyclePill label="Stable" count={counts.stable_warm} dot="bg-lifecycle-stable_warm" />
            <LifecyclePill label="Declining" count={counts.declining_warm} dot="bg-lifecycle-declining_warm" />
            <LifecyclePill label="Churned" count={counts.churned_warm} dot="bg-lifecycle-churned_warm" />
            <LifecyclePill label="Cold start" count={counts.cold_start} dot="bg-lifecycle-cold_start" />
          </div>
        </div>
      </div>

      {customers.length === 0 ? (
        <div className="px-5 pb-6">
          <EmptyState
            icon={Users}
            title="No customers assigned"
            description="This seller has no customers in their portfolio yet."
          />
        </div>
      ) : (
        <div className="border-t border-slate-100">
          <div className="divide-y divide-slate-100">
            {visible.map((c) => (
              <CustomerRow key={c.cust_id} customer={c} onClick={() => onClickCustomer(c)} />
            ))}
          </div>
          {customers.length > INITIAL ? (
            <div className="border-t border-slate-100">
              <button
                type="button"
                onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
                className="w-full px-5 py-2.5 text-xs font-semibold text-mck-blue hover:text-mck-blue-dark hover:bg-mck-sky/40 transition-colors flex items-center justify-center gap-1.5"
              >
                {expanded === 1 ? (
                  <>
                    <ChevronUp size={14} />
                    Show less
                  </>
                ) : (
                  <>
                    <ChevronDown size={14} />
                    Show all {customers.length} customers ({hidden} more)
                  </>
                )}
              </button>
            </div>
          ) : null}
        </div>
      )}
    </Card>
  );
}

function CustomerRow({ customer, onClick }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className="w-full text-left px-5 py-3 hover:bg-mck-sky/30 transition-colors flex items-center gap-4"
    >
      <div className="flex-shrink-0 w-9 h-9 rounded-full bg-mck-sky text-mck-blue font-semibold text-xs flex items-center justify-center">
        {(customer.customer_name || `${customer.cust_id}`).slice(0, 2).toUpperCase()}
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-semibold text-mck-navy truncate">
            {customer.customer_name || `Customer ${customer.cust_id}`}
          </span>
          <span className="text-[10px] text-slate-400">#{customer.cust_id}</span>
        </div>
        <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-x-2 gap-y-0.5 flex-wrap">
          {customer.segment ? <span>{customer.segment}</span> : null}
          {customer.market_code ? (
            <>
              <span className="text-slate-300">|</span>
              <span>{customer.market_code}</span>
            </>
          ) : null}
          {customer.specialty_code ? (
            <>
              <span className="text-slate-300">|</span>
              <span>Specialty {customer.specialty_code}</span>
            </>
          ) : null}
        </div>
      </div>
      <div className="flex-shrink-0">
        {customer.status ? <LifecycleBadge status={customer.status} /> : null}
      </div>
      <ChevronRight size={16} className="text-slate-400 flex-shrink-0" />
    </button>
  );
}

function RecentSalesBySeller({ sales, loading }) {
  const [expanded, setExpanded] = useState(0);
  const INITIAL = 10;

  if (loading) {
    return (
      <Card>
        <CardHeader title="Recent sales by this seller" subtitle="Latest activity attributed to this seller" />
        <FullPanelSpinner label="Loading sales" />
      </Card>
    );
  }

  const visible = expanded === 1 ? sales : sales.slice(0, INITIAL);
  const hidden = sales.length - INITIAL;

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3">
        <h2 className="text-base font-semibold text-mck-navy">Recent sales by this seller</h2>
        <p className="text-xs text-slate-500 mt-0.5">
          {sales.length} sales (within last 200 platform-wide)
          {expanded === 0 && sales.length > INITIAL ? <> &middot; showing top {INITIAL}</> : null}
        </p>
      </div>

      {sales.length === 0 ? (
        <div className="px-5 pb-6">
          <EmptyState
            icon={Package}
            title="No recent sales"
            description="This seller has not had any sales recorded in the most recent 200 platform-wide transactions."
          />
        </div>
      ) : (
        <div className="border-t border-slate-100">
          <div className="divide-y divide-slate-100">
            {visible.map((row) => (
              <SaleRow key={row.purchase_id} row={row} />
            ))}
          </div>
          {sales.length > INITIAL ? (
            <div className="border-t border-slate-100">
              <button
                type="button"
                onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
                className="w-full px-5 py-2.5 text-xs font-semibold text-mck-blue hover:text-mck-blue-dark hover:bg-mck-sky/40 transition-colors flex items-center justify-center gap-1.5"
              >
                {expanded === 1 ? (
                  <>
                    <ChevronUp size={14} />
                    Show less
                  </>
                ) : (
                  <>
                    <ChevronDown size={14} />
                    Show all {sales.length} sales ({hidden} more)
                  </>
                )}
              </button>
            </div>
          ) : null}
        </div>
      )}
    </Card>
  );
}

function SaleRow({ row }) {
  const isRec = row.from_recommendation === true || row.from_recommendation === 1;
  const sourceCode = row.recommendation_source && row.recommendation_source.code;
  return (
    <div className="px-5 py-3 hover:bg-slate-50/50 flex items-start gap-3">
      <div
        className={`flex-shrink-0 w-9 h-9 rounded-md flex items-center justify-center ${
          isRec ? 'bg-mck-orange/10 text-mck-orange' : 'bg-slate-100 text-slate-500'
        }`}
      >
        <ShoppingBag size={16} />
      </div>
      <div className="flex-1 min-w-0">
        <div className="flex items-start justify-between gap-3 flex-wrap">
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2 flex-wrap">
              <span className="text-sm font-semibold text-mck-navy truncate">{row.item_description || `Item ${row.item_id}`}</span>
              {isRec ? <SignalBadge signal={sourceCode} size="xs" /> : null}
            </div>
            <div className="text-xs text-slate-500 mt-0.5">
              {row.customer_name || `Cust ${row.cust_id}`}
              {row.family ? <> &middot; {row.family}</> : null}
            </div>
          </div>
          <div className="text-right flex-shrink-0">
            <div className="text-sm font-bold text-mck-navy">{formatCurrency(row.line_total)}</div>
            <div className="text-[10px] text-slate-400 mt-0.5">{relativeTime(row.sold_at)}</div>
          </div>
        </div>
      </div>
    </div>
  );
}

function LifecyclePill({ label, count, dot }) {
  if (!count) return null;
  return (
    <span className="inline-flex items-center gap-1 text-[10px] font-medium text-slate-700 bg-slate-100 px-2 py-0.5 rounded">
      <span className={`w-1.5 h-1.5 rounded-full ${dot}`} />
      {label} {count}
    </span>
  );
}

function Field({ icon: Icon, label, value }) {
  return (
    <div className="flex items-start gap-2 min-w-0">
      <Icon size={13} className="text-slate-400 flex-shrink-0 mt-0.5" />
      <div className="min-w-0">
        <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">{label}</div>
        <div className="text-xs text-mck-navy font-medium truncate">{value}</div>
      </div>
    </div>
  );
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
