import { useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { Sparkles, Award, ArrowRightLeft, Plus, Loader2 } from 'lucide-react';
import { getCart, getCartHelper, addToCart } from '../../api.js';
import Card from '../ui/Card.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import ProductImage from '../ui/ProductImage.jsx';
import { cartGroupKeySet, filterSuggestionsAgainstCart } from '../../lib/variantStem.js';
import { formatCurrency, formatPercent } from '../../lib/format.js';

// Cart helper panel

// Fetches the customer's cart, then calls /recommendations/cart-helper
// with that list. Renders three sections:
//   1. Cart complements (frequently bought together with current cart items)
//   2. Private brand upgrades (McKesson Brand swaps for current cart items)
//   3. Medline-to-McKesson conversions
// Each suggestion has its own pitch_reason and an Add-to-cart button.
//
// Two important behaviors:
//   - Add buttons pass valid backend cart_items.source values (the legacy
//     strings like 'cart_complement_high_lift' don't exist in the DB enum).
//   - Suggestions are filtered against the cart's variant-group keys so we
//     don't suggest a different size of something already in cart.

export default function CartHelperPanel({ custId }) {
  const queryClient = useQueryClient();
  const [addingItemId, setAddingItemId] = useState(null);

  // 1. Fetch cart so we know which items to send to cart-helper
  const cartQuery = useQuery({
    queryKey: ['cart', custId],
    queryFn: () => getCart(custId),
    enabled: Boolean(custId)
  });

  const cartItems = useMemo(
    () => (cartQuery.data && cartQuery.data.items) || [],
    [cartQuery.data]
  );
  const cartItemIds = useMemo(() => cartItems.map((i) => i.item_id), [cartItems]);

  // Group keys for items already in cart - used to filter out variant
  // suggestions (e.g. don't recommend a 4x4 gauze pad if a 2x2 of the same
  // family is already in the cart).
  const cartGroupKeys = useMemo(() => cartGroupKeySet(cartItems), [cartItems]);

  // 2. Once we have cart items, fire cart-helper
  const helperQuery = useQuery({
    queryKey: ['cart-helper', custId, cartItemIds.join(',')],
    queryFn: () => getCartHelper(custId, cartItemIds),
    enabled: Boolean(custId) && cartItemIds.length > 0
  });

  const addMutation = useMutation({
    mutationFn: ({ itemId, source }) => addToCart(custId, itemId, 1, source),
    onMutate: ({ itemId }) => setAddingItemId(itemId),
    onSettled: () => {
      setAddingItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
      queryClient.invalidateQueries({ queryKey: ['cart-helper', custId] });
    }
  });

  function handleAdd(itemId, source) {
    addMutation.mutate({ itemId, source });
  }

  if (cartQuery.isLoading) {
    return null;
  }

  // No cart -> no helper
  if (cartItemIds.length === 0) {
    return (
      <Card>
        <EmptyState
          icon={Sparkles}
          title="Cart-helper waiting"
          description="Add items to the cart and live suggestions will appear here."
        />
      </Card>
    );
  }

  if (helperQuery.isLoading) {
    return (
      <Card>
        <div className="flex items-center justify-center py-6 text-sm text-slate-500">
          <Loader2 size={16} className="animate-spin mr-2" />
          Computing live suggestions...
        </div>
      </Card>
    );
  }

  if (helperQuery.isError || !helperQuery.data) {
    return (
      <Card>
        <EmptyState title="Could not load cart-helper" description="Please try again shortly." />
      </Card>
    );
  }

  const data = helperQuery.data;

  // Filter each suggestion list against the cart's variant-group keys.
  const complements = filterSuggestionsAgainstCart(
    data.cart_complements || [],
    cartGroupKeys
  );
  const pbUpgradesRaw = data.private_brand_upgrades || [];
  // PB upgrades use pb_item_id / pb_description as the "outgoing" item; we
  // need to filter on those, not the cart_item_id that the suggestion is
  // replacing.
  const pbUpgrades = filterSuggestionsAgainstCart(
    pbUpgradesRaw.map((s) => ({
      ...s,
      item_id: s.pb_item_id,
      description: s.pb_description
    })),
    cartGroupKeys
  );
  const medlineRaw = data.medline_conversions || [];
  // The Medline schema uses mckesson_item_id / mckesson_description for the
  // suggested swap. Some older response shapes use item_id; handle both.
  const medline = filterSuggestionsAgainstCart(
    medlineRaw.map((s) => ({
      ...s,
      item_id: s.mckesson_item_id || s.item_id,
      description: s.mckesson_description || s.description
    })),
    cartGroupKeys
  );

  const total = complements.length + pbUpgrades.length + medline.length;
  const rawTotal =
    (data.cart_complements || []).length +
    (data.private_brand_upgrades || []).length +
    (data.medline_conversions || []).length;
  // Distinguish "backend returned nothing" from "we filtered everything out"
  // so the empty state can give the user something useful.
  const backendEmpty = rawTotal === 0;
  const allFiltered = rawTotal > 0 && total === 0;

  if (total === 0) {
    return (
      <Card>
        <EmptyState
          icon={Sparkles}
          title={
            backendEmpty
              ? 'No live suggestions for this cart'
              : 'All suggestions are already covered'
          }
          description={
            backendEmpty
              ? `The cart has ${cartItemIds.length} item${cartItemIds.length === 1 ? '' : 's'} but the engine did not find complementary products, private-brand upgrades, or Medline conversions for them yet. Try adding items from a different category.`
              : `Found ${rawTotal} suggestion${rawTotal === 1 ? '' : 's'} but every one is a different size or variant of something already in this cart. Add a product from a new category to see fresh ideas.`
          }
        />
      </Card>
    );
  }

  return (
    <div className="space-y-5">
      <div className="bg-gradient-to-r from-mck-orange/10 via-mck-blue/5 to-transparent border border-mck-orange/30 rounded-lg px-4 py-3">
        <div className="flex items-center gap-2">
          <Sparkles size={16} className="text-mck-orange" />
          <span className="text-sm font-semibold text-mck-navy">Live cart-helper</span>
          <span className="text-xs text-slate-500">
            {total} suggestion{total === 1 ? '' : 's'} based on cart of {data.cart_size || cartItemIds.length}
          </span>
        </div>
      </div>

      {complements.length > 0 ? (
        <SuggestionGroup
          icon={Plus}
          title="Frequently bought together"
          subtitle="Common pairings with items already in the cart"
          items={complements.map((s) => ({
            ...s,
            kind: 'complement',
            display_name: s.description,
            display_subtitle: s.trigger_description ? `Pairs with: ${s.trigger_description}` : null,
            metric_label: 'Lift',
            metric_value: formatLift(s.lift)
          }))}
          source="recommendation_cart_complement"
          addingItemId={addingItemId}
          onAdd={handleAdd}
        />
      ) : null}

      {pbUpgrades.length > 0 ? (
        <SuggestionGroup
          icon={Award}
          title="McKesson Brand alternatives"
          subtitle="Private-brand equivalents of items in the cart"
          items={pbUpgrades.map((s) => ({
            ...s,
            kind: 'pb_upgrade',
            item_id: s.pb_item_id,
            description: s.pb_description,
            display_name: s.pb_description,
            display_subtitle: s.cart_item_description ? `Swap for: ${s.cart_item_description}` : null,
            metric_label: 'Savings',
            metric_value: s.estimated_savings_pct ? formatPercentValue(s.estimated_savings_pct) : '-'
          }))}
          source="recommendation_pb_upgrade"
          addingItemId={addingItemId}
          onAdd={handleAdd}
        />
      ) : null}

      {medline.length > 0 ? (
        <SuggestionGroup
          icon={ArrowRightLeft}
          title="Medline-to-McKesson conversions"
          subtitle="McKesson alternatives to Medline products in the cart"
          items={medline.map((s) => ({
            ...s,
            kind: 'medline',
            item_id: s.mckesson_item_id || s.item_id,
            description: s.mckesson_description || s.description,
            display_name: s.mckesson_description || s.description,
            display_subtitle: s.medline_description ? `Convert from: ${s.medline_description}` : null,
            metric_label: 'Savings',
            metric_value: s.estimated_savings_pct ? formatPercentValue(s.estimated_savings_pct) : '-'
          }))}
          source="recommendation_medline_conversion"
          addingItemId={addingItemId}
          onAdd={handleAdd}
        />
      ) : null}
    </div>
  );
}

function SuggestionGroup({ icon: Icon, title, subtitle, items, source, addingItemId, onAdd }) {
  if (!items || items.length === 0) return null;
  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 border-b border-slate-100">
        <div className="flex items-center gap-2">
          <Icon size={16} className="text-mck-blue" />
          <h3 className="text-sm font-semibold text-mck-navy">{title}</h3>
          <span className="text-xs text-slate-500">({items.length})</span>
        </div>
        <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>
      </div>
      <div className="divide-y divide-slate-100">
        {items.map((s, i) => (
          <SuggestionRow
            key={`${s.item_id || s.pb_item_id || s.mckesson_item_id}-${i}`}
            suggestion={s}
            source={source}
            adding={addingItemId === (s.item_id || s.pb_item_id || s.mckesson_item_id) ? 1 : 0}
            onAdd={onAdd}
          />
        ))}
      </div>
    </Card>
  );
}

function SuggestionRow({ suggestion, source, adding, onAdd }) {
  const itemId = suggestion.item_id || suggestion.pb_item_id || suggestion.mckesson_item_id;
  const stockOut =
    suggestion.units_in_stock !== undefined &&
    suggestion.units_in_stock !== null &&
    suggestion.units_in_stock <= 0;
  const isMckesson = suggestion.is_mckesson_brand === true || suggestion.is_mckesson_brand === 1;

  return (
    <div className="px-5 py-3 flex items-start gap-3 hover:bg-slate-50/40">
      <ProductImage size="sm" alt={suggestion.display_name} />

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-semibold text-mck-navy truncate">{suggestion.display_name}</span>
          {isMckesson ? (
            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-mck-orange bg-orange-50 border border-orange-200 px-1.5 py-0.5 rounded">
              McKesson Brand
            </span>
          ) : null}
        </div>
        {suggestion.display_subtitle ? (
          <div className="text-[11px] text-slate-500 mt-0.5">{suggestion.display_subtitle}</div>
        ) : null}
        {suggestion.pitch_reason ? (
          <div className="text-xs text-mck-navy mt-1 italic leading-snug border-l-2 border-mck-orange pl-2">
            {suggestion.pitch_reason}
          </div>
        ) : null}
      </div>

      <div className="text-right flex-shrink-0">
        <div className="text-sm font-bold text-mck-navy">{formatCurrency(suggestion.median_unit_price)}</div>
        <div className="text-[10px] text-slate-400 mt-0.5">{suggestion.metric_label}: {suggestion.metric_value}</div>
        <button
          type="button"
          onClick={() => onAdd(itemId, source)}
          disabled={adding === 1 || stockOut}
          className="mt-2 inline-flex items-center gap-1 text-xs font-semibold text-white bg-mck-blue hover:bg-mck-blue-dark disabled:bg-slate-300 disabled:cursor-not-allowed px-2.5 py-1 rounded"
        >
          {adding === 1 ? <Loader2 size={11} className="animate-spin" /> : <Plus size={11} />}
          {stockOut ? 'Out of stock' : 'Add'}
        </button>
      </div>
    </div>
  );
}

function formatLift(lift) {
  if (lift === null || lift === undefined) return '-';
  const n = typeof lift === 'string' ? parseFloat(lift) : lift;
  if (Number.isNaN(n)) return '-';
  return `${n.toFixed(2)}x`;
}

// estimated_savings_pct comes back as a number that's already in percent
// units (e.g. 12.5 means 12.5%), so we don't multiply by 100 here.
function formatPercentValue(value) {
  if (value === null || value === undefined) return '-';
  const n = typeof value === 'string' ? parseFloat(value) : value;
  if (Number.isNaN(n)) return '-';
  return `${n.toFixed(1)}%`;
}
