import { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Filter, RefreshCw, Sparkles, Check } from 'lucide-react';
import { getCustomerRecommendations, addToCart } from '../../api.js';
import { listSignals, signalToCartSource } from '../../lib/signals.js';
import { listPurposes } from '../../lib/purposes.js';
import { groupItems } from '../../lib/variantStem.js';
import { useAuth } from '../../auth.jsx';
import RecommendationCard from './RecommendationCard.jsx';
import VariantPickerModal from '../catalog/VariantPickerModal.jsx';
import RejectRecommendationModal from './RejectRecommendationModal.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';

// Recommendation list

// Fetches top 20 recommendations for a customer and renders them grouped
// by product family. Sellers can also reject recs they don't find useful;
// rejections are logged backend-side for engine improvement.
//
// Reject flow:
//   - "Reject" button is shown only when the logged-in user is a seller
//   - clicking opens RejectRecommendationModal which writes to the API
//   - rejected items are added to a session-local "rejected" set so they
//     disappear from the list immediately and stay hidden until the page
//     is refreshed (we don't refetch since the rec engine response is
//     precomputed and won't reflect the rejection until a re-run).

export default function RecommendationList({ custId, allowAddToCart = 1 }) {
  const queryClient = useQueryClient();
  const { user } = useAuth();
  const isSeller = (user && user.role) === 'seller';

  const [signalFilter, setSignalFilter] = useState(new Set());
  const [purposeFilter, setPurposeFilter] = useState(new Set());
  const [openGroup, setOpenGroup] = useState(null);
  const [justAddedKeys, setJustAddedKeys] = useState({});

  // Rejection state
  const [rejectTarget, setRejectTarget] = useState(null);   // rec object to reject
  const [rejectedItemIds, setRejectedItemIds] = useState(() => new Set());

  const { data, isLoading, isError, error, refetch, isFetching } = useQuery({
    queryKey: ['recs', 'customer', custId],
    queryFn: () => getCustomerRecommendations(custId, 20),
    enabled: Boolean(custId)
  });

  const addMutation = useMutation({
    mutationFn: ({ itemId, quantity, source }) =>
      addToCart(custId, itemId, quantity, source),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
      queryClient.invalidateQueries({ queryKey: ['cart-helper'] });
    }
  });

  function handleAddVariant(itemId, quantity, source) {
    return addMutation.mutateAsync({ itemId, quantity, source });
  }

  const rawRecs = (data && data.recommendations) || [];

  // Filter out items already rejected this session before grouping so a
  // multi-variant family doesn't lose its lead unexpectedly when only one
  // variant was rejected.
  const visibleRecs = useMemo(
    () => rawRecs.filter((r) => !rejectedItemIds.has(r.item_id)),
    [rawRecs, rejectedItemIds]
  );

  const groupedRecs = useMemo(() => buildGroupedRecs(visibleRecs), [visibleRecs]);

  const presentSignals = useMemo(() => {
    const set = new Set();
    visibleRecs.forEach((r) => {
      if (r.primary_signal) set.add(r.primary_signal);
    });
    return listSignals().filter((s) => set.has(s.key));
  }, [visibleRecs]);

  const presentPurposes = useMemo(() => {
    const set = new Set();
    visibleRecs.forEach((r) => {
      if (r.rec_purpose) set.add(r.rec_purpose);
    });
    return listPurposes().filter((p) => set.has(p.key));
  }, [visibleRecs]);

  const filtered = useMemo(() => {
    return groupedRecs.filter((g) => {
      if (signalFilter.size > 0) {
        const anyMatch = g.variants.some((v) => signalFilter.has(v.primary_signal));
        if (!anyMatch) return 0;
      }
      if (purposeFilter.size > 0) {
        const anyMatch = g.variants.some((v) => purposeFilter.has(v.rec_purpose));
        if (!anyMatch) return 0;
      }
      return 1;
    });
  }, [groupedRecs, signalFilter, purposeFilter]);

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

  function handleAdd(group) {
    if (group.variantCount === 1) {
      const lead = group.lead;
      const source = signalToCartSource(lead.primary_signal);
      addMutation.mutate(
        { itemId: lead.item_id, quantity: 1, source },
        {
          onSuccess: () => {
            setJustAddedKeys((prev) => ({ ...prev, [group.key]: 1 }));
            setTimeout(() => {
              setJustAddedKeys((prev) => {
                const next = { ...prev };
                delete next[group.key];
                return next;
              });
            }, 2000);
          }
        }
      );
      return;
    }
    setOpenGroup(group);
  }

  function handleCloseModal() {
    if (openGroup) {
      setJustAddedKeys((prev) => ({ ...prev, [openGroup.key]: 1 }));
      setTimeout(() => {
        setJustAddedKeys((prev) => {
          const next = { ...prev };
          delete next[openGroup.key];
          return next;
        });
      }, 2000);
    }
    setOpenGroup(null);
  }

  // Reject flow handlers

  function handleRejectClick(rec) {
    setRejectTarget(rec);
  }

  function handleRejectClose() {
    setRejectTarget(null);
  }

  function handleRejectSuccess(rec) {
    // Add to session-local hidden set
    setRejectedItemIds((prev) => {
      const next = new Set(prev);
      next.add(rec.item_id);
      return next;
    });
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

  if (rawRecs.length === 0) {
    return (
      <EmptyState
        icon={Sparkles}
        title="No recommendations available"
        description="The recommendation engine has not produced results for this customer yet."
      />
    );
  }

  const filtersActive = signalFilter.size > 0 || purposeFilter.size > 0;
  const modalSource = openGroup ? signalToCartSource(openGroup.lead.primary_signal) : 'manual';
  const rejectedCount = rejectedItemIds.size;

  return (
    <div className="space-y-4">
      <div className="bg-white border border-slate-200 rounded-lg p-4">
        <div className="flex items-center justify-between mb-3 flex-wrap gap-2">
          <div className="flex items-center gap-2 text-xs font-semibold text-slate-600 uppercase tracking-wider">
            <Filter size={13} />
            Filters
          </div>
          <div className="flex items-center gap-2">
            <span className="text-xs text-slate-500">
              Showing {filtered.length} of {groupedRecs.length} product families ({visibleRecs.length} SKUs
              {rejectedCount > 0 ? `, ${rejectedCount} rejected this session` : ''})
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
          {filtered.map((group) => (
            <RecommendationCard
              key={group.key}
              rec={group.lead}
              variantCount={group.variantCount}
              canReject={isSeller ? 1 : 0}
              onReject={isSeller ? handleRejectClick : undefined}
              onAddToCart={
                allowAddToCart === 1
                  ? () => handleAdd(group)
                  : undefined
              }
              addingToCart={
                addMutation.isPending && addMutation.variables && addMutation.variables.itemId === group.lead.item_id
                  ? 1
                  : 0
              }
              justAdded={justAddedKeys[group.key] === 1 ? 1 : 0}
            />
          ))}
        </div>
      )}

      <VariantPickerModal
        open={openGroup !== null}
        onClose={handleCloseModal}
        group={openGroup}
        custId={custId}
        source={modalSource}
        onAddVariant={handleAddVariant}
      />

      <RejectRecommendationModal
        open={rejectTarget !== null}
        onClose={handleRejectClose}
        rec={rejectTarget}
        custId={custId}
        onSuccess={handleRejectSuccess}
      />
    </div>
  );
}

function buildGroupedRecs(recs) {
  const groups = groupItems(recs);
  const enriched = groups.map((g) => {
    const sortedVariants = [...g.variants]
      .sort((a, b) => (a.rank || 999) - (b.rank || 999))
      .map((v) => ({
        ...v,
        unit_price: v.unit_price !== undefined && v.unit_price !== null ? v.unit_price : v.median_unit_price
      }));
    const lead = sortedVariants[0];
    return {
      key: g.key,
      lead,
      variants: sortedVariants,
      variantCount: g.variantCount,
      representative: lead
    };
  });
  enriched.sort((a, b) => (a.lead.rank || 999) - (b.lead.rank || 999));
  return enriched;
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
