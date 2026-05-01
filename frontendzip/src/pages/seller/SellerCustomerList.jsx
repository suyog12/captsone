import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import {
  Search,
  AlertOctagon,
  TrendingDown,
  TrendingUp,
  Sparkles,
  Users,
  Globe,
  UserCheck,
  UserPlus
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import LifecycleBadge from '../../components/ui/LifecycleBadge.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import CreateCustomerRecordModal from '../../components/customer-mgmt/CreateCustomerRecordModal.jsx';
import { getMyCustomers, filterCustomers } from '../../api.js';
import { useAuth } from '../../auth.jsx';
import {
  segmentLabel,
  specialtyLabel,
  marketLabel
} from '../../lib/customerLabels.js';

// Seller customer list

const PAGE_SIZE = 200;

export default function SellerCustomerList() {
  const navigate = useNavigate();
  const { user } = useAuth();
  const [scope, setScope] = useState('mine');
  const [query, setQuery] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [createOpen, setCreateOpen] = useState(0);

  const myCustomersQuery = useQuery({
    queryKey: ['seller', 'me', 'customers'],
    queryFn: () => getMyCustomers(500, 0)
  });

  const allCustomersQuery = useQuery({
    queryKey: ['seller', 'all-customers', statusFilter],
    queryFn: () => {
      const params = { scope: 'all', limit: PAGE_SIZE };
      if (statusFilter !== 'all') params.status = statusFilter;
      return filterCustomers(params);
    },
    enabled: scope === 'all'
  });

  const myCustomers = (myCustomersQuery.data && myCustomersQuery.data.items) || [];

  const counts = useMemo(() => {
    const out = {
      stable_warm: 0,
      declining_warm: 0,
      churned_warm: 0,
      cold_start: 0,
      total: myCustomers.length
    };
    myCustomers.forEach((c) => {
      if (c.status && out[c.status] !== undefined) out[c.status] += 1;
    });
    return out;
  }, [myCustomers]);

  const activeRows = scope === 'mine' ? myCustomers : (allCustomersQuery.data || []);
  const activeLoading = scope === 'mine' ? myCustomersQuery.isLoading : allCustomersQuery.isLoading;
  const activeError = scope === 'mine' ? myCustomersQuery.isError : allCustomersQuery.isError;

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    return activeRows.filter((c) => {
      if (scope === 'mine' && statusFilter !== 'all' && c.status !== statusFilter) return 0;
      if (!q) return 1;
      const hay = `${c.customer_name || ''} ${c.cust_id} ${c.segment || ''} ${c.specialty_code || ''} ${c.market_code || ''}`.toLowerCase();
      return hay.indexOf(q) !== -1 ? 1 : 0;
    });
  }, [activeRows, query, statusFilter, scope]);

  const myUserId = user && user.user_id;
  const annotated = useMemo(() => {
    return filtered.map((c) => {
      const assignedToMe = c.assigned_seller_id === myUserId;
      const unassigned = c.assigned_seller_id === null || c.assigned_seller_id === undefined;
      return {
        ...c,
        _relationship: assignedToMe ? 'mine' : unassigned ? 'unassigned' : 'other'
      };
    });
  }, [filtered, myUserId]);

  function handleCreated(customer) {
    // After creating, navigate into the new customer's profile so the
    // seller can immediately set up their first cart / look at recs.
    if (customer && customer.cust_id) {
      navigate(`/seller/customers/${customer.cust_id}`);
    }
  }

  return (
    <AppShell
      title="My customers"
      subtitle="Your assigned portfolio"
      actions={
        <button
          type="button"
          onClick={() => setCreateOpen(1)}
          className="inline-flex items-center gap-1.5 text-xs font-semibold bg-mck-blue text-white px-3 py-1.5 rounded hover:bg-mck-blue-dark"
        >
          <UserPlus size={13} />
          Add customer
        </button>
      }
    >
      <div className="space-y-6">
        {/* KPI strip - pinned to my customers regardless of tab */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
          <StatCard label="Total" value={counts.total} icon={Users} accent="mck-navy" />
          <StatCard label="Stable" value={counts.stable_warm} icon={TrendingUp} accent="green" />
          <StatCard label="Declining" value={counts.declining_warm} icon={TrendingDown} accent="mck-orange" />
          <StatCard label="Churned" value={counts.churned_warm} icon={AlertOctagon} accent="red" />
          <StatCard label="Cold Start" value={counts.cold_start} icon={Sparkles} accent="mck-blue" />
        </div>

        <Card padding="none">
          <div className="border-b border-slate-200 px-4 pt-3">
            <div className="flex gap-1 -mb-px">
              <ScopeTab
                active={scope === 'mine'}
                onClick={() => setScope('mine')}
                icon={UserCheck}
                label="My customers"
                count={myCustomers.length}
              />
              <ScopeTab
                active={scope === 'all'}
                onClick={() => setScope('all')}
                icon={Globe}
                label="All customers"
                count={scope === 'all' ? (allCustomersQuery.data || []).length : null}
              />
            </div>
          </div>

          <div className="px-4 py-3 border-b border-slate-100 flex items-center gap-2 flex-wrap">
            <div className="relative flex-1 min-w-[16rem]">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search by name, cust_id, segment, specialty..."
                className="w-full pl-9 pr-3 py-2 text-sm border border-slate-200 rounded-md focus:border-mck-blue focus:ring-1 focus:ring-mck-blue/20 outline-none"
              />
            </div>
            <div className="flex items-center gap-1.5">
              <FilterChip
                active={statusFilter === 'all'}
                onClick={() => setStatusFilter('all')}
                label="All"
              />
              <FilterChip
                active={statusFilter === 'stable_warm'}
                onClick={() => setStatusFilter('stable_warm')}
                label="Stable"
                dot="bg-green-500"
              />
              <FilterChip
                active={statusFilter === 'declining_warm'}
                onClick={() => setStatusFilter('declining_warm')}
                label="Declining"
                dot="bg-mck-orange"
              />
              <FilterChip
                active={statusFilter === 'churned_warm'}
                onClick={() => setStatusFilter('churned_warm')}
                label="Churned"
                dot="bg-red-500"
              />
              <FilterChip
                active={statusFilter === 'cold_start'}
                onClick={() => setStatusFilter('cold_start')}
                label="Cold Start"
                dot="bg-mck-blue"
              />
            </div>
          </div>

          {activeLoading ? (
            <FullPanelSpinner label="Loading customers" />
          ) : activeError ? (
            <EmptyState
              title="Could not load customers"
              description="Please try again in a moment."
            />
          ) : annotated.length === 0 ? (
            <EmptyState
              icon={Users}
              title={
                scope === 'mine'
                  ? 'No customers in your portfolio yet'
                  : 'No customers match your filters'
              }
              description={
                scope === 'mine'
                  ? 'Click Add customer above to create your first account.'
                  : 'Try clearing the search or status filter.'
              }
            />
          ) : (
            <ul className="divide-y divide-slate-100">
              {annotated.map((c, idx) => (
                <CustomerRow
                  key={c.cust_id}
                  index={idx + 1}
                  customer={c}
                  onClick={() => navigate(`/seller/customers/${c.cust_id}`)}
                />
              ))}
            </ul>
          )}
        </Card>
      </div>

      <CreateCustomerRecordModal
        open={createOpen === 1}
        onClose={() => setCreateOpen(0)}
        onCreated={handleCreated}
      />
    </AppShell>
  );
}

function ScopeTab({ active, onClick, icon: Icon, label, count }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors flex items-center gap-2 ${
        active
          ? 'border-mck-blue text-mck-blue'
          : 'border-transparent text-slate-500 hover:text-mck-navy hover:border-slate-300'
      }`}
    >
      <Icon size={14} />
      <span>{label}</span>
      {count !== null && count !== undefined ? (
        <span
          className={`ml-1 inline-flex items-center justify-center min-w-[1.25rem] px-1.5 py-0.5 rounded-full text-[10px] font-semibold ${
            active ? 'bg-mck-blue/10 text-mck-blue' : 'bg-slate-100 text-slate-600'
          }`}
        >
          {count}
        </span>
      ) : null}
    </button>
  );
}

function CustomerRow({ customer, onClick, index }) {
  const relationship = customer._relationship;
  return (
    <li>
      <button
        type="button"
        onClick={onClick}
        className="w-full px-4 py-3 hover:bg-slate-50 transition-colors flex items-center gap-3 text-left"
      >
        {/* Row index avatar - sequential 1, 2, 3, ... for the current view */}
        <div className="flex-shrink-0 w-10 h-10 rounded-md bg-mck-sky text-mck-blue font-bold text-sm flex items-center justify-center tabular-nums">
          {index}
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-mck-navy truncate">
              {customer.customer_name || `Customer ${customer.cust_id}`}
            </span>
            <span className="text-[11px] text-slate-400 font-mono">#{customer.cust_id}</span>
          </div>
          <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-2 flex-wrap">
            <span>{segmentLabel(customer.segment)}</span>
            <span className="text-slate-300">|</span>
            <span>{specialtyLabel(customer.specialty_code)}</span>
            <span className="text-slate-300">|</span>
            <span>{marketLabel(customer.market_code)}</span>
            {relationship === 'unassigned' ? (
              <>
                <span className="text-slate-300">|</span>
                <span className="text-mck-blue font-medium">Unassigned</span>
              </>
            ) : null}
            {relationship === 'other' ? (
              <>
                <span className="text-slate-300">|</span>
                <span className="text-slate-400 italic">Assigned elsewhere</span>
              </>
            ) : null}
          </div>
        </div>

        <div className="flex-shrink-0">
          <LifecycleBadge status={customer.status} size="sm" />
        </div>
      </button>
    </li>
  );
}

function FilterChip({ active, onClick, label, dot }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1 rounded-full border transition-colors ${
        active === 1 || active === true
          ? 'bg-mck-blue text-white border-mck-blue'
          : 'bg-white text-slate-600 border-slate-200 hover:border-slate-300'
      }`}
    >
      {dot ? <span className={`w-1.5 h-1.5 rounded-full ${active ? 'bg-white' : dot}`} /> : null}
      {label}
    </button>
  );
}
