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
  UserX,
  UserPlus,
  ChevronLeft,
  ChevronRight,
  X
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import LifecycleBadge from '../../components/ui/LifecycleBadge.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import CreateCustomerRecordModal from '../../components/customer-mgmt/CreateCustomerRecordModal.jsx';
import AddLoginModal from '../../components/customer-mgmt/AddLoginModal.jsx';
import { getMyCustomers, filterCustomers, searchCustomers } from '../../api.js';
import { useAuth } from '../../auth.jsx';
import {
  segmentLabel,
  specialtyLabel,
  marketLabel
} from '../../lib/customerLabels.js';

// Seller customer list

const PAGE_SIZE_OPTIONS = [25, 50, 100, 250];

const STATUS_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'stable_warm', label: 'Stable', dot: 'bg-green-500' },
  { value: 'declining_warm', label: 'Declining', dot: 'bg-mck-orange' },
  { value: 'churned_warm', label: 'Churned', dot: 'bg-red-500' },
  { value: 'cold_start', label: 'Cold Start', dot: 'bg-mck-blue' }
];

const ACCOUNT_OPTIONS = [
  { value: 'all', label: 'All accounts' },
  { value: 'users', label: 'Has login (users)' },
  { value: 'no_users', label: 'No login (records only)' }
];

export default function SellerCustomerList() {
  const navigate = useNavigate();
  const { user } = useAuth();

  const [scope, setScope] = useState('mine');
  const [query, setQuery] = useState('');
  const [submittedSearch, setSubmittedSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [accountFilter, setAccountFilter] = useState('all');
  const [page, setPage] = useState(0);
  const [pageSize, setPageSize] = useState(50);
  const [createOpen, setCreateOpen] = useState(0);
  const [addLoginCustomer, setAddLoginCustomer] = useState(null);

  // Tab 1: My customers (client-side filter)
  const myCustomersQuery = useQuery({
    queryKey: ['seller', 'me', 'customers'],
    queryFn: () => getMyCustomers(500, 0)
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

  // Tab 2: All customers (server-side filter + pagination, OR search mode)
  // When the seller submits a free-text query in All Customers, we switch
  // from /customers/filter to /customers/search.
  const inSearchMode = scope === 'all' && submittedSearch.trim().length > 0;

  const allCustomersParams = useMemo(() => {
    const p = { scope: 'all', limit: pageSize, offset: page * pageSize };
    if (statusFilter !== 'all') p.status = statusFilter;
    if (accountFilter !== 'all') p.account_status = accountFilter;
    return p;
  }, [pageSize, page, statusFilter, accountFilter]);

  const allCustomersQuery = useQuery({
    queryKey: ['seller', 'all-customers', allCustomersParams],
    queryFn: () => filterCustomers(allCustomersParams),
    enabled: scope === 'all' && !inSearchMode,
    keepPreviousData: true
  });

  const allCustomersSearchQuery = useQuery({
    queryKey: ['seller', 'all-customers-search', submittedSearch],
    queryFn: () => searchCustomers(submittedSearch.trim(), 100, 'all'),
    enabled: inSearchMode
  });

  const activeLoading = scope === 'mine'
    ? myCustomersQuery.isLoading
    : (inSearchMode ? allCustomersSearchQuery.isLoading : allCustomersQuery.isLoading);
  const activeError = scope === 'mine'
    ? myCustomersQuery.isError
    : (inSearchMode ? allCustomersSearchQuery.isError : allCustomersQuery.isError);

  const allCustomersData = allCustomersQuery.data || { total: 0, items: [] };
  const allCustomersTotal = allCustomersData.total || 0;

  // Search results are a flat list (not paginated), so no page math
  const searchResults = Array.isArray(allCustomersSearchQuery.data) ? allCustomersSearchQuery.data : [];

  const totalPages = (scope === 'all' && !inSearchMode) ? Math.max(1, Math.ceil(allCustomersTotal / pageSize)) : 1;
  const hasPrev = scope === 'all' && !inSearchMode && page > 0;
  const hasNext = scope === 'all' && !inSearchMode && page < totalPages - 1;

  let activeRows;
  if (scope === 'mine') {
    activeRows = myCustomers || [];
  } else if (inSearchMode) {
    activeRows = searchResults;
  } else {
    activeRows = Array.isArray(allCustomersData.items) ? allCustomersData.items : [];
  }

  const filtered = useMemo(() => {
    if (scope !== 'mine') return activeRows;
    const q = query.trim().toLowerCase();
    return activeRows.filter((c) => {
      if (statusFilter !== 'all' && c.status !== statusFilter) return 0;
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
    if (customer && customer.cust_id) {
      navigate(`/seller/customers/${customer.cust_id}`);
    }
  }

  function handleScopeChange(next) {
    setScope(next);
    setPage(0);
    setQuery('');
    setSubmittedSearch('');
  }

  function handleSearchSubmit(e) {
    e.preventDefault();
    setSubmittedSearch(query);
    setPage(0);
  }

  function clearSearch() {
    setQuery('');
    setSubmittedSearch('');
    setPage(0);
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
                onClick={() => handleScopeChange('mine')}
                icon={UserCheck}
                label="My customers"
                count={myCustomers.length}
              />
              <ScopeTab
                active={scope === 'all'}
                onClick={() => handleScopeChange('all')}
                icon={Globe}
                label="All customers"
                count={scope === 'all' ? allCustomersTotal : null}
              />
            </div>
          </div>

          <div className="px-4 py-3 border-b border-slate-100 flex items-center gap-2 flex-wrap">
            <form onSubmit={handleSearchSubmit} className="relative flex-1 min-w-[16rem]">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
              <input
                type="text"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder={scope === 'mine'
                  ? 'Search by name, cust_id, segment, specialty...'
                  : 'Search by cust_id, market code, specialty, segment, or business name (press Enter)'}
                className="w-full pl-9 pr-9 py-2 text-sm border border-slate-200 rounded-md focus:border-mck-blue focus:ring-1 focus:ring-mck-blue/20 outline-none"
              />
              {query ? (
                <button
                  type="button"
                  onClick={clearSearch}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
                >
                  <X size={14} />
                </button>
              ) : null}
            </form>
            <div className="flex items-center gap-1.5">
              {STATUS_OPTIONS.map((o) => (
                <FilterChip
                  key={o.value}
                  active={statusFilter === o.value}
                  onClick={() => { setStatusFilter(o.value); setPage(0); }}
                  label={o.label}
                  dot={o.dot}
                />
              ))}
            </div>
            {scope === 'all' && !inSearchMode ? (
              <select
                value={accountFilter}
                onChange={(e) => { setAccountFilter(e.target.value); setPage(0); }}
                className="text-sm bg-white border border-slate-200 rounded-md px-3 py-1.5 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue"
              >
                {ACCOUNT_OPTIONS.map((o) => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            ) : null}
          </div>

          {inSearchMode ? (
            <div className="px-4 py-2 border-b border-slate-100 text-xs text-slate-500 inline-flex items-center gap-1.5 bg-mck-sky">
              <Search size={11} />
              Searching for &quot;{submittedSearch}&quot; across the customer base &middot; status and account filters disabled in search mode
            </div>
          ) : null}

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
            <>
              {scope === 'all' && !inSearchMode ? (
                <div className="px-4 py-2 border-b border-slate-100 flex items-center justify-between flex-wrap gap-2">
                  <div className="text-xs text-slate-500">
                    Showing {page * pageSize + 1} to {Math.min((page + 1) * pageSize, allCustomersTotal)} of {allCustomersTotal.toLocaleString()} customers
                  </div>
                  <div className="flex items-center gap-2 text-xs text-slate-500">
                    <label htmlFor="seller-page-size" className="text-slate-500">Rows per page</label>
                    <select
                      id="seller-page-size"
                      value={pageSize}
                      onChange={(e) => { setPageSize(Number(e.target.value)); setPage(0); }}
                      className="text-xs bg-white border border-slate-200 rounded px-2 py-1 text-mck-navy focus:outline-none focus:ring-1 focus:ring-mck-blue"
                    >
                      {PAGE_SIZE_OPTIONS.map((n) => (
                        <option key={n} value={n}>{n}</option>
                      ))}
                    </select>
                  </div>
                </div>
              ) : inSearchMode ? (
                <div className="px-4 py-2 border-b border-slate-100 text-xs text-slate-500">
                  {searchResults.length} matches
                </div>
              ) : null}

              <ul className="divide-y divide-slate-100">
                {annotated.map((c, idx) => (
                  <CustomerRow
                    key={c.cust_id}
                    index={idx + 1 + (scope === 'all' && !inSearchMode ? page * pageSize : 0)}
                    customer={c}
                    onClick={() => navigate(`/seller/customers/${c.cust_id}`)}
                    onAddLogin={() => setAddLoginCustomer(c)}
                  />
                ))}
              </ul>

              {scope === 'all' && !inSearchMode ? (
                <div className="border-t border-slate-100 px-4 py-3 flex items-center justify-between">
                  <button
                    type="button"
                    onClick={() => setPage(Math.max(0, page - 1))}
                    disabled={!hasPrev}
                    className="text-xs font-medium text-slate-600 hover:text-mck-navy disabled:text-slate-300 disabled:cursor-not-allowed inline-flex items-center gap-1 px-3 py-1.5 rounded hover:bg-slate-50 disabled:hover:bg-transparent"
                  >
                    <ChevronLeft size={14} />
                    Previous
                  </button>
                  <span className="text-xs text-slate-500">
                    Page {page + 1} of {totalPages.toLocaleString()}
                  </span>
                  <button
                    type="button"
                    onClick={() => setPage(page + 1)}
                    disabled={!hasNext}
                    className="text-xs font-medium text-slate-600 hover:text-mck-navy disabled:text-slate-300 disabled:cursor-not-allowed inline-flex items-center gap-1 px-3 py-1.5 rounded hover:bg-slate-50 disabled:hover:bg-transparent"
                  >
                    Next
                    <ChevronRight size={14} />
                  </button>
                </div>
              ) : null}
            </>
          )}
        </Card>
      </div>

      <CreateCustomerRecordModal
        open={createOpen === 1}
        onClose={() => setCreateOpen(0)}
        onCreated={handleCreated}
      />

      <AddLoginModal
        open={addLoginCustomer !== null}
        customer={addLoginCustomer}
        onClose={() => setAddLoginCustomer(null)}
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
          {typeof count === 'number' ? count.toLocaleString() : count}
        </span>
      ) : null}
    </button>
  );
}

function UserBadge({ has }) {
  if (has) {
    return (
      <span className="inline-flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wide text-emerald-700 bg-emerald-50 border border-emerald-200 px-1.5 py-0.5 rounded">
        <UserCheck size={10} />
        User
      </span>
    );
  }
  return (
    <span className="inline-flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wide text-slate-500 bg-slate-100 border border-slate-200 px-1.5 py-0.5 rounded">
      <UserX size={10} />
      Not a User
    </span>
  );
}

function CustomerRow({ customer, onClick, onAddLogin, index }) {
  const relationship = customer._relationship;
  // Sellers can only add a login to customers assigned to them.
  // Admins-side modal handles all cases via AdminCustomers.
  const canAddLogin = relationship === 'mine' && !customer.has_user_account;

  function handleAddLoginClick(e) {
    e.stopPropagation();
    onAddLogin();
  }

  return (
    <li className="flex items-stretch hover:bg-slate-50 transition-colors">
      <button
        type="button"
        onClick={onClick}
        className="flex-1 px-4 py-3 flex items-center gap-3 text-left min-w-0"
      >
        <div className="flex-shrink-0 w-10 h-10 rounded-md bg-mck-sky text-mck-blue font-bold text-sm flex items-center justify-center tabular-nums">
          {index}
        </div>

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="text-sm font-semibold text-mck-navy truncate">
              {customer.customer_name || `Customer ${customer.cust_id}`}
            </span>
            <span className="text-[11px] text-slate-400 font-mono">#{customer.cust_id}</span>
            <UserBadge has={customer.has_user_account} />
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

      {canAddLogin ? (
        <div className="flex items-center pr-4">
          <button
            type="button"
            onClick={handleAddLoginClick}
            className="inline-flex items-center gap-1 text-[11px] font-semibold bg-mck-blue text-white px-2.5 py-1 rounded hover:bg-mck-blue-dark"
            title="Create a dashboard login for this customer"
          >
            <UserPlus size={11} />
            Add Login
          </button>
        </div>
      ) : null}
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