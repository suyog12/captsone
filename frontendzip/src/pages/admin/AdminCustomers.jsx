import { useState, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import {
  Building2,
  Search,
  Filter,
  X,
  ChevronRight,
  ChevronLeft,
  AlertOctagon,
  TrendingDown,
  TrendingUp,
  Sparkles,
  Users
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import LifecycleBadge from '../../components/ui/LifecycleBadge.jsx';
import SegmentedControl from '../../components/ui/SegmentedControl.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import { filterCustomers, searchCustomers } from '../../api.js';
import { listLifecycles } from '../../lib/lifecycle.js';

// Admin customers

const PAGE_SIZE = 25;

const STATUS_OPTIONS = [
  { value: 'all', label: 'All statuses' },
  { value: 'stable_warm', label: 'Stable' },
  { value: 'declining_warm', label: 'Declining' },
  { value: 'churned_warm', label: 'Churned' },
  { value: 'cold_start', label: 'Cold start' }
];

const MARKET_OPTIONS = [
  { value: 'all', label: 'All markets' },
  { value: 'PO', label: 'PO' },
  { value: 'SC', label: 'SC' },
  { value: 'LTC', label: 'LTC' },
  { value: 'HH', label: 'HH' },
  { value: 'AC', label: 'AC' }
];

export default function AdminCustomers() {
  const navigate = useNavigate();
  const [search, setSearch] = useState('');
  const [submittedSearch, setSubmittedSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState('all');
  const [marketFilter, setMarketFilter] = useState('all');
  const [page, setPage] = useState(0);

  // When the user has typed a search query and submitted it, we use the
  // /customers/search endpoint instead of /customers/filter.
  const inSearchMode = submittedSearch.trim().length > 0;

  const filterParams = useMemo(() => {
    const p = { limit: PAGE_SIZE, offset: page * PAGE_SIZE };
    if (statusFilter !== 'all') p.status = statusFilter;
    if (marketFilter !== 'all') p.market_code = marketFilter;
    return p;
  }, [statusFilter, marketFilter, page]);

  const filterQuery = useQuery({
    queryKey: ['admin', 'customers-filter', filterParams],
    queryFn: () => filterCustomers(filterParams),
    enabled: !inSearchMode
  });

  const searchQuery = useQuery({
    queryKey: ['admin', 'customers-search', submittedSearch],
    queryFn: () => searchCustomers(submittedSearch.trim(), 100),
    enabled: inSearchMode
  });

  const isLoading = inSearchMode ? searchQuery.isLoading : filterQuery.isLoading;
  const isError = inSearchMode ? searchQuery.isError : filterQuery.isError;
  const customers = inSearchMode ? (searchQuery.data || []) : (filterQuery.data || []);

  function handleSearchSubmit(e) {
    e.preventDefault();
    setSubmittedSearch(search);
    setPage(0);
  }

  function clearSearch() {
    setSearch('');
    setSubmittedSearch('');
    setPage(0);
  }

  function clearAllFilters() {
    clearSearch();
    setStatusFilter('all');
    setMarketFilter('all');
    setPage(0);
  }

  const filtersActive = statusFilter !== 'all' || marketFilter !== 'all' || inSearchMode;
  const hasMore = !inSearchMode && customers.length === PAGE_SIZE;
  const hasPrev = !inSearchMode && page > 0;

  return (
    <AppShell title="Customers" subtitle="Browse the full customer base">
      <div className="space-y-6">
        {/* Filter bar */}
        <Card>
          <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
            <div className="flex items-center gap-2 text-xs font-semibold text-slate-600 uppercase tracking-wider">
              <Filter size={13} />
              Filters
            </div>
            {filtersActive ? (
              <button
                type="button"
                onClick={clearAllFilters}
                className="text-xs text-mck-blue hover:text-mck-blue-dark px-2 py-1 rounded hover:bg-mck-sky"
              >
                Clear all filters
              </button>
            ) : null}
          </div>

          <div className="flex items-center gap-3 flex-wrap">
            <form onSubmit={handleSearchSubmit} className="relative flex-1 min-w-[16rem]">
              <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search by cust_id, market code, specialty, or segment..."
                className="w-full pl-9 pr-9 py-2 text-sm border border-slate-200 rounded-md focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue placeholder:text-slate-400"
              />
              {search ? (
                <button
                  type="button"
                  onClick={clearSearch}
                  className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
                >
                  <X size={14} />
                </button>
              ) : null}
            </form>

            <select
              value={statusFilter}
              onChange={(e) => {
                setStatusFilter(e.target.value);
                setPage(0);
              }}
              disabled={inSearchMode}
              className="text-sm bg-white border border-slate-200 rounded-md px-3 py-2 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue disabled:bg-slate-50 disabled:text-slate-400"
            >
              {STATUS_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>

            <select
              value={marketFilter}
              onChange={(e) => {
                setMarketFilter(e.target.value);
                setPage(0);
              }}
              disabled={inSearchMode}
              className="text-sm bg-white border border-slate-200 rounded-md px-3 py-2 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue disabled:bg-slate-50 disabled:text-slate-400"
            >
              {MARKET_OPTIONS.map((o) => (
                <option key={o.value} value={o.value}>{o.label}</option>
              ))}
            </select>
          </div>

          {inSearchMode ? (
            <div className="mt-3 text-xs text-slate-500 inline-flex items-center gap-1.5 bg-mck-sky px-2.5 py-1 rounded">
              <Search size={11} />
              Searching for &quot;{submittedSearch}&quot; &middot; status and market filters disabled in search mode
            </div>
          ) : null}
        </Card>

        {/* Results */}
        {isLoading ? (
          <FullPanelSpinner label="Loading customers" />
        ) : isError ? (
          <EmptyState title="Could not load customers" description="Please try again." />
        ) : customers.length === 0 ? (
          <EmptyState
            icon={Building2}
            title={inSearchMode ? 'No customers match your search' : 'No customers match your filters'}
            description="Try adjusting or clearing the filters above."
            action={
              filtersActive ? (
                <button
                  type="button"
                  onClick={clearAllFilters}
                  className="px-3 py-1.5 text-sm bg-mck-blue text-white rounded hover:bg-mck-blue-dark"
                >
                  Clear filters
                </button>
              ) : null
            }
          />
        ) : (
          <Card padding="none">
            <div className="px-5 py-3 border-b border-slate-100 flex items-center justify-between flex-wrap gap-2">
              <div className="text-xs text-slate-500">
                {inSearchMode
                  ? `${customers.length} matches`
                  : `Showing page ${page + 1} (${customers.length} customers)`}
              </div>
            </div>
            <div className="divide-y divide-slate-100">
              {customers.map((c) => (
                <CustomerRow
                  key={c.cust_id}
                  customer={c}
                  onClick={() => navigate(`/admin/customers/${c.cust_id}`)}
                />
              ))}
            </div>

            {!inSearchMode ? (
              <div className="border-t border-slate-100 px-5 py-3 flex items-center justify-between">
                <button
                  type="button"
                  onClick={() => setPage(Math.max(0, page - 1))}
                  disabled={!hasPrev}
                  className="text-xs font-medium text-slate-600 hover:text-mck-navy disabled:text-slate-300 disabled:cursor-not-allowed inline-flex items-center gap-1 px-3 py-1.5 rounded hover:bg-slate-50 disabled:hover:bg-transparent"
                >
                  <ChevronLeft size={14} />
                  Previous
                </button>
                <span className="text-xs text-slate-500">Page {page + 1}</span>
                <button
                  type="button"
                  onClick={() => setPage(page + 1)}
                  disabled={!hasMore}
                  className="text-xs font-medium text-slate-600 hover:text-mck-navy disabled:text-slate-300 disabled:cursor-not-allowed inline-flex items-center gap-1 px-3 py-1.5 rounded hover:bg-slate-50 disabled:hover:bg-transparent"
                >
                  Next
                  <ChevronRight size={14} />
                </button>
              </div>
            ) : null}
          </Card>
        )}
      </div>
    </AppShell>
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
          {customer.specialty_code ? (
            <>
              <span className="text-slate-300">|</span>
              <span>Specialty {customer.specialty_code}</span>
            </>
          ) : null}
          {customer.market_code ? (
            <>
              <span className="text-slate-300">|</span>
              <span>{customer.market_code}</span>
            </>
          ) : null}
          {customer.archetype ? (
            <>
              <span className="text-slate-300">|</span>
              <span className="capitalize">{customer.archetype.replace(/_/g, ' ')}</span>
            </>
          ) : null}
          {customer.assigned_seller_id ? (
            <>
              <span className="text-slate-300">|</span>
              <span>Seller #{customer.assigned_seller_id}</span>
            </>
          ) : (
            <>
              <span className="text-slate-300">|</span>
              <span className="text-amber-600">Unassigned</span>
            </>
          )}
        </div>
      </div>

      <div className="flex-shrink-0">
        <LifecycleBadge status={customer.status} />
      </div>

      <ChevronRight size={16} className="text-slate-400 flex-shrink-0" />
    </button>
  );
}
