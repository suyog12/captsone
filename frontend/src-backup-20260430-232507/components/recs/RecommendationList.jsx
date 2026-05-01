import { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Filter, RefreshCw, Sparkles, Check } from 'lucide-react';
import { getCustomerRecommendations, addToCart } from '../../api.js';
import { listSignals } from '../../lib/signals.js';
import { listPurposes } from '../../lib/purposes.js';
import RecommendationCard from './RecommendationCard.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';

// Recommendation list

// Fetches top 20 recommendations for a customer and lets the user
// filter client-side by signal type and rec_purpose. Filter chips only
// appear for signal/purpose values actually present in this customer's
// data, so the UI never shows an empty filter for, say, lapsed_recovery
// when the customer has no win-back recs.

export default function RecommendationList({ custId, allowAddToCart = 1 }) {
  const queryClient = useQueryClient();
  const [signalFilter, setSignalFilter] = useState(new Set());
  const [purposeFilter, setPurposeFilter] = useState(new Set());
  const [addingItemId, setAddingItemId] = useState(null);

  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['recs', 'customer', custId],
    queryFn: () => getCustomerRecommendations(custId, 20),
    enabled: Boolean(custId)
  });

  // The source argument carries the per-signal cart_items.source value
  // computed by the card. We pass it straight through to the API so each
  // signal's conversion rate can be tracked independently.
  const addToCartMutation = useMutation({
    mutationFn: ({ itemId, source }) => addToCart(custId, itemId, 1, source),
    onMutate: ({ itemId }) => setAddingItemId(itemId),
    onSettled: () => {
      setAddingItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
      queryClient.invalidateQueries({ queryKey: ['cart-helper'] });
    }
  });

  const recs = (data && data.recommendations) || [];

  // Build filter chips only from signals/purposes actually present
  const presentSignals = useMemo(() => {
    const set = new Set();
    recs.forEach((r) => {
      if (r.primary_signal) set.add(r.primary_signal);
    });
    return listSignals().filter((s) => set.has(s.key));
  }, [recs]);

  const presentPurposes = useMemo(() => {
    const set = new Set();
    recs.forEach((r) => {
      if (r.rec_purpose) set.add(r.rec_purpose);
    });
    return listPurposes().filter((p) => set.has(p.key));
  }, [recs]);

  // Apply filters
  const filtered = useMemo(() => {
    return recs.filter((r) => {
      if (signalFilter.size > 0 && !signalFilter.has(r.primary_signal)) return 0;
      if (purposeFilter.size > 0 && !purposeFilter.has(r.rec_purpose)) return 0;
      return 1;
    });
  }, [recs, signalFilter, purposeFilter]);

  function toggleSignal(key) {
    const next = new Set(signalFilter);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setSignalFilter(next);
  }

  function togglePurpose(key) {
    const next = new Set(purposeFilter);
    if (next.has(key)) next.delete(key);
    else next.add(key);
    setPurposeFilter(next);
  }

  function clearFilters() {
    setSignalFilter(new Set());
    setPurposeFilter(new Set());
  }

  // Card calls this with (rec, source) where source already comes from
  // signalToCartSource(rec.primary_signal). We just forward it.
  function handleAdd(rec, source) {
    addToCartMutation.mutate({ itemId: rec.item_id, source });
  }

  if (isLoading) return <FullPanelSpinner label="Loading recommendations" />;

  if (isError) {
    const detail = error && error.response && error.response.data && error.response.data.detail;
    const msg = (typeof detail === 'string' && detail) || (error && error.message) || 'Failed to load recommendations.';
    return (
      <EmptyState
        title="Could not load recommendations"
        description={msg}
        action={
          <button
            type="button"
            onClick={() => refetch()}
            className="px-3 py-1.5 text-sm bg-mck-blue text-white rounded hover:bg-mck-blue-dark"
          >
            Retry
          </button>
        }
      />
    );
  }

  if (recs.length === 0) {
    return (
      <EmptyState
        icon={Sparkles}
        title="No recommendations available"
        description="The recommendation engine has not produced results for this customer yet."
      />
    );
  }

  const filtersActive = signalFilter.size > 0 || purposeFilter.size > 0;

  return (
    <div className="space-y-4">
      {/* Filter bar */}
      <div className="bg-white border border-slate-200 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
          <div className="flex items-center gap-2 text-xs font-semibold text-slate-600 uppercase tracking-wider">
            <Filter size={13} />
            Filters
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-500">
              Showing {filtered.length} of {recs.length}
            </span>
            <button
              type="button"
              onClick={() => refetch()}
              disabled={isFetching}
              className="text-xs text-slate-500 hover:text-mck-navy flex items-center gap-1 px-2 py-1 rounded hover:bg-slate-100"
            >
              <RefreshCw size={12} className={isFetching ? 'animate-spin' : ''} />
              Refresh
            </button>
            {filtersActive ? (
              <button
                type="button"
                onClick={clearFilters}
                className="text-xs text-mck-blue hover:text-mck-blue-dark px-2 py-1 rounded hover:bg-mck-sky"
              >
                Clear filters
              </button>
            ) : null}
          </div>
        </div>

        <div className="space-y-2">
          {presentSignals.length > 0 ? (
            <FilterRow
              label="Signal"
              chips={presentSignals.map((s) => ({ key: s.key, label: s.label, color: s.color, icon: s.icon }))}
              active={signalFilter}
              onToggle={toggleSignal}
            />
          ) : null}
          {presentPurposes.length > 0 ? (
            <FilterRow
              label="Purpose"
              chips={presentPurposes.map((p) => ({ key: p.key, label: p.label, color: p.color, icon: p.icon }))}
              active={purposeFilter}
              onToggle={togglePurpose}
            />
          ) : null}
        </div>
      </div>

      {/* Result list */}
      {filtered.length === 0 ? (
        <EmptyState
          title="No matches"
          description="No recommendations match the selected filters. Try clearing one of them."
          action={
            <button
              type="button"
              onClick={clearFilters}
              className="px-3 py-1.5 text-sm bg-mck-blue text-white rounded hover:bg-mck-blue-dark"
            >
              Clear filters
            </button>
          }
        />
      ) : (
        <div className="space-y-3">
          {filtered.map((rec) => (
            <RecommendationCard
              key={rec.item_id}
              rec={rec}
              onAddToCart={allowAddToCart === 1 ? handleAdd : undefined}
              addingToCart={addingItemId === rec.item_id ? 1 : 0}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function FilterRow({ label, chips, active, onToggle }) {
  return (
    <div className="flex items-start gap-3">
      <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider pt-1.5 min-w-[3.5rem]">
        {label}
      </div>
      <div className="flex flex-wrap gap-1.5 flex-1">
        {chips.map((c) => {
          const isActive = active.has(c.key);
          const Icon = c.icon;
          return (
            <button
              key={c.key}
              type="button"
              onClick={() => onToggle(c.key)}
              className={`inline-flex items-center gap-1 text-xs font-medium px-2 py-1 rounded-full border transition-all ${
                isActive
                  ? `${c.color} ring-2 ring-offset-1 ring-mck-blue/40`
                  : 'bg-white text-slate-600 border-slate-200 hover:border-slate-300'
              }`}
            >
              {isActive ? (
                <Check size={11} />
              ) : Icon ? (
                <Icon size={11} className="text-slate-400" />
              ) : null}
              {c.label}
            </button>
          );
        })}
      </div>
    </div>
  );
}
