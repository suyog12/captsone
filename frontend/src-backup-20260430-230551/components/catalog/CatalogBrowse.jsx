import { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Search,
  Package,
  ShoppingCart,
  Check,
  Loader2,
  X,
  Filter,
  AlertCircle,
  ChevronLeft,
  ChevronRight,
  Award
} from 'lucide-react';
import Card from '../ui/Card.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import ProductImage from '../ui/ProductImage.jsx';
import { browseProducts, getProductFilters, addToCart } from '../../api.js';
import { formatCurrency, formatNumber } from '../../lib/format.js';

// Catalog browse

const PAGE_SIZE = 24;

/**
 * Catalog browse, shared by all three roles.
 *
 * Props:
 *   custId   - cust_id to add items to. Required when allowAddToCart=1.
 *              For customers, pass their own cust_id. For sellers, pass the
 *              currently selected customer's cust_id. For admins, can pass
 *              null and addToCart will be hidden.
 *   allowAddToCart - 1 = show Add buttons, 0 = read-only browse
 *   ctaSource      - source string passed to addToCart. Default: 'manual'.
 */
export default function CatalogBrowse({ custId = null, allowAddToCart = 0, ctaSource = 'manual' }) {
  const queryClient = useQueryClient();

  const [query, setQuery] = useState('');
  const [submittedQuery, setSubmittedQuery] = useState('');
  const [familyFilter, setFamilyFilter] = useState('');
  const [categoryFilter, setCategoryFilter] = useState('');
  const [supplierFilter, setsupplierFilter] = useState('');
  const [privateBrandOnly, setprivateBrandOnly] = useState(0);
  const [inStockOnly, setInStockOnly] = useState(1);
  const [page, setPage] = useState(0);

  // Track which cards are mid-add and which were just added (for the badge)
  const [adding, setAdding] = useState({});
  const [justAdded, setJustAdded] = useState({});

  const filtersQuery = useQuery({
    queryKey: ['products', 'filters'],
    queryFn: getProductFilters,
    staleTime: 5 * 60 * 1000
  });

  const browseParams = useMemo(() => {
    const p = { limit: PAGE_SIZE, offset: page * PAGE_SIZE };
    if (submittedQuery) p.q = submittedQuery;
    if (familyFilter) p.family = familyFilter;
    if (categoryFilter) p.category = categoryFilter;
    if (supplierFilter) p.brand = supplierFilter;
    if (privateBrandOnly) p.is_private_brand = true;
    if (inStockOnly) p.in_stock_only = true;
    return p;
  }, [submittedQuery, familyFilter, categoryFilter, supplierFilter, privateBrandOnly, inStockOnly, page]);

  const browseQuery = useQuery({
    queryKey: ['products', 'browse', browseParams],
    queryFn: () => browseProducts(browseParams),
    keepPreviousData: true
  });

  const addMutation = useMutation({
    mutationFn: ({ itemId, quantity }) =>
      addToCart(custId, itemId, quantity, ctaSource),
    onSuccess: (_data, vars) => {
      // Invalidate carts so other panels refresh
      queryClient.invalidateQueries({ queryKey: ['cart'] });
      queryClient.invalidateQueries({ queryKey: ['cart', 'me'] });
      queryClient.invalidateQueries({ queryKey: ['customer', custId, 'cart'] });
      // Mark as just-added for visual feedback
      setJustAdded((prev) => ({ ...prev, [vars.itemId]: 1 }));
      setAdding((prev) => ({ ...prev, [vars.itemId]: 0 }));
      setTimeout(() => {
        setJustAdded((prev) => {
          const next = { ...prev };
          delete next[vars.itemId];
          return next;
        });
      }, 2000);
    },
    onError: (_err, vars) => {
      setAdding((prev) => ({ ...prev, [vars.itemId]: 0 }));
    }
  });

  function handleAdd(item) {
    if (!custId) return;
    setAdding((prev) => ({ ...prev, [item.item_id]: 1 }));
    addMutation.mutate({ itemId: item.item_id, quantity: 1 });
  }

  function handleSearchSubmit(e) {
    e.preventDefault();
    setSubmittedQuery(query);
    setPage(0);
  }

  function clearFilters() {
    setQuery('');
    setSubmittedQuery('');
    setFamilyFilter('');
    setCategoryFilter('');
    setsupplierFilter('');
    setprivateBrandOnly(0);
    setInStockOnly(1);
    setPage(0);
  }

  const items = (browseQuery.data && browseQuery.data.items) || [];
  const total = (browseQuery.data && browseQuery.data.total) || 0;
  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const filtersActive =
    submittedQuery || familyFilter || categoryFilter || supplierFilter || privateBrandOnly === 1 || inStockOnly !== 1;

  const filterOptions = filtersQuery.data || { families: [], categories: [], suppliers: [] };

  return (
    <div className="space-y-4">
      {/* Search + filters bar */}
      <Card padding="md">
        <form onSubmit={handleSearchSubmit} className="flex items-center gap-2 flex-wrap mb-3">
          <div className="relative flex-1 min-w-[16rem]">
            <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="Search products by name, family, category, brand, or item ID..."
              className="w-full pl-9 pr-3 py-2 text-sm border border-slate-200 rounded-md focus:border-mck-blue focus:ring-1 focus:ring-mck-blue/20 outline-none"
            />
          </div>
          <button
            type="submit"
            className="px-4 py-2 bg-mck-blue text-white text-sm font-medium rounded hover:bg-mck-blue-dark"
          >
            Search
          </button>
        </form>

        <div className="flex items-center gap-2 flex-wrap">
          <FilterSelect
            value={familyFilter}
            onChange={(v) => { setFamilyFilter(v); setPage(0); }}
            options={filterOptions.families}
            placeholder="All families"
            label="Family"
          />
          <FilterSelect
            value={categoryFilter}
            onChange={(v) => { setCategoryFilter(v); setPage(0); }}
            options={filterOptions.categories}
            placeholder="All categories"
            label="Category"
          />
          <FilterSelect
            value={supplierFilter}
            onChange={(v) => { setsupplierFilter(v); setPage(0); }}
            options={filterOptions.suppliers}
            placeholder="All suppliers"
            label="Supplier"
          />
          <ToggleChip
            active={privateBrandOnly === 1}
            onClick={() => { setprivateBrandOnly(privateBrandOnly === 1 ? 0 : 1); setPage(0); }}
            icon={Award}
            label="Private Brand"
          />
          <ToggleChip
            active={inStockOnly === 1}
            onClick={() => { setInStockOnly(inStockOnly === 1 ? 0 : 1); setPage(0); }}
            icon={Package}
            label="In stock"
          />
          {filtersActive ? (
            <button
              type="button"
              onClick={clearFilters}
              className="inline-flex items-center gap-1 text-xs font-medium text-slate-500 hover:text-mck-navy ml-auto"
            >
              <X size={12} /> Clear
            </button>
          ) : null}
        </div>
      </Card>

      {/* Result count and pagination header */}
      <div className="flex items-center justify-between text-xs text-slate-500 px-1">
        <div>
          {browseQuery.isLoading ? 'Loading...' : <>Showing <span className="font-semibold text-mck-navy">{items.length}</span> of <span className="font-semibold text-mck-navy">{formatNumber(total)}</span> products</>}
        </div>
        {totalPages > 1 ? (
          <div className="flex items-center gap-2">
            <button
              type="button"
              disabled={page === 0}
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              className="p-1 rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <ChevronLeft size={14} />
            </button>
            <span>
              Page <span className="font-semibold text-mck-navy">{page + 1}</span> of {totalPages}
            </span>
            <button
              type="button"
              disabled={page >= totalPages - 1}
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              className="p-1 rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              <ChevronRight size={14} />
            </button>
          </div>
        ) : null}
      </div>

      {/* Grid */}
      {browseQuery.isLoading ? (
        <FullPanelSpinner label="Loading catalog" />
      ) : browseQuery.isError ? (
        <EmptyState
          icon={AlertCircle}
          title="Could not load catalog"
          description="Please try again in a moment."
        />
      ) : items.length === 0 ? (
        <EmptyState
          icon={Package}
          title="No products match your filters"
          description={filtersActive ? 'Try clearing filters or searching for something else.' : 'The catalog appears to be empty.'}
        />
      ) : (
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
          {items.map((item) => (
            <ProductCard
              key={item.item_id}
              item={item}
              allowAddToCart={allowAddToCart}
              custId={custId}
              adding={adding[item.item_id] === 1}
              justAdded={justAdded[item.item_id] === 1}
              onAdd={() => handleAdd(item)}
            />
          ))}
        </div>
      )}

      {/* Bottom pagination */}
      {totalPages > 1 ? (
        <div className="flex items-center justify-center gap-2 pt-2">
          <button
            type="button"
            disabled={page === 0}
            onClick={() => { setPage((p) => Math.max(0, p - 1)); window.scrollTo({ top: 0, behavior: 'smooth' }); }}
            className="px-3 py-1.5 text-xs font-medium border border-slate-200 rounded hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed inline-flex items-center gap-1"
          >
            <ChevronLeft size={12} /> Previous
          </button>
          <span className="text-xs text-slate-500 px-2">
            Page {page + 1} of {totalPages}
          </span>
          <button
            type="button"
            disabled={page >= totalPages - 1}
            onClick={() => { setPage((p) => Math.min(totalPages - 1, p + 1)); window.scrollTo({ top: 0, behavior: 'smooth' }); }}
            className="px-3 py-1.5 text-xs font-medium border border-slate-200 rounded hover:bg-slate-50 disabled:opacity-40 disabled:cursor-not-allowed inline-flex items-center gap-1"
          >
            Next <ChevronRight size={12} />
          </button>
        </div>
      ) : null}
    </div>
  );
}

function ProductCard({ item, allowAddToCart, custId, adding, justAdded, onAdd }) {
  const inStock = item.units_in_stock > 0;
  const lowStock = inStock && item.units_in_stock < 20;

  return (
    <div className="bg-white border border-slate-200 rounded-lg overflow-hidden hover:shadow-md hover:border-mck-blue/30 transition-all flex flex-col">
      {/* Image */}
      <div className="aspect-square bg-slate-50 flex items-center justify-center p-4 relative">
        <ProductImage imageUrl={null} item={item} size="lg" />
        {item.is_private_brand ? (
          <div className="absolute top-2 left-2 inline-flex items-center gap-1 px-2 py-0.5 bg-mck-orange/10 text-mck-orange text-[10px] font-bold uppercase tracking-wider rounded-full border border-mck-orange/30">
            <Award size={10} />
            McKesson
          </div>
        ) : null}
      </div>

      {/* Body */}
      <div className="p-3 flex-1 flex flex-col">
        <div className="flex-1 mb-2">
          <div className="text-xs text-slate-500 font-mono mb-1">SKU {item.item_id}</div>
          <div className="text-sm font-semibold text-mck-navy line-clamp-2 leading-snug">
            {item.description || `Item ${item.item_id}`}
          </div>
          {item.family || item.category ? (
            <div className="text-xs text-slate-500 mt-1 truncate">
              {item.family}{item.family && item.category ? ' | ' : ''}{item.category}
            </div>
          ) : null}
          {item.brand && !item.is_private_brand ? (
            <div className="text-[11px] text-slate-400 mt-0.5 truncate">{item.brand}</div>
          ) : null}
        </div>

        {/* Price + stock */}
        <div className="flex items-end justify-between mb-3">
          <div>
            <div className="text-lg font-bold text-mck-navy leading-none">
              {item.unit_price !== null ? formatCurrency(item.unit_price) : '-'}
            </div>
            <div className={`text-[11px] mt-1 ${inStock ? (lowStock ? 'text-mck-orange' : 'text-green-600') : 'text-red-500'}`}>
              {inStock ? `${formatNumber(item.units_in_stock)} in stock${lowStock ? ' (low)' : ''}` : 'Out of stock'}
            </div>
          </div>
        </div>

        {/* Action button */}
        {allowAddToCart === 1 ? (
          <button
            type="button"
            onClick={onAdd}
            disabled={!custId || !inStock || adding}
            className={`w-full inline-flex items-center justify-center gap-1.5 px-3 py-2 text-xs font-semibold rounded transition-colors ${
              justAdded
                ? 'bg-green-500 text-white'
                : !inStock
                ? 'bg-slate-100 text-slate-400 cursor-not-allowed'
                : 'bg-mck-blue text-white hover:bg-mck-blue-dark disabled:opacity-60'
            }`}
          >
            {adding ? (
              <>
                <Loader2 size={12} className="animate-spin" />
                Adding...
              </>
            ) : justAdded ? (
              <>
                <Check size={12} />
                Added to cart
              </>
            ) : !inStock ? (
              'Out of stock'
            ) : (
              <>
                <ShoppingCart size={12} />
                Add to cart
              </>
            )}
          </button>
        ) : (
          <div className="w-full text-center px-3 py-2 text-[11px] text-slate-400 italic border border-slate-100 rounded">
            Read-only view
          </div>
        )}
      </div>
    </div>
  );
}

function FilterSelect({ value, onChange, options, placeholder, label }) {
  return (
    <label className="inline-flex items-center gap-1.5">
      <span className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{label}</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        className="text-xs border border-slate-200 rounded px-2 py-1.5 bg-white focus:border-mck-blue focus:ring-1 focus:ring-mck-blue/20 outline-none min-w-[8rem]"
      >
        <option value="">{placeholder}</option>
        {(options || []).map((opt) => (
          <option key={opt} value={opt}>
            {opt}
          </option>
        ))}
      </select>
    </label>
  );
}

function ToggleChip({ active, onClick, icon: Icon, label }) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`inline-flex items-center gap-1.5 text-xs font-medium px-2.5 py-1.5 rounded-full border transition-colors ${
        active
          ? 'bg-mck-blue text-white border-mck-blue'
          : 'bg-white text-slate-600 border-slate-200 hover:border-slate-300'
      }`}
    >
      <Icon size={12} />
      {label}
    </button>
  );
}
