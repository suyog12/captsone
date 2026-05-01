import { useMemo, useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { AlertTriangle, Package, RefreshCw, Loader2, Check } from 'lucide-react';
import Card from '../ui/Card.jsx';
import { browseProducts, addToCart } from '../../api.js';
import { formatNumber, formatCurrency } from '../../lib/format.js';

// Inventory gauges

// Surfaces low-stock items in the customer's most-shopped product families
// so they can reorder before running out. Each row shows a stock-level
// gauge bar and a one-click Reorder button that adds quantity 1 directly
// to the cart with source='manual'.
//
// We pick the family with the largest revenue share from top_families and
// query that family's catalog for in-stock items, then surface those whose
// units_in_stock is below the LOW_STOCK_THRESHOLD.

const LOW_STOCK_THRESHOLD = 30;
const MAX_VISIBLE = 5;

export default function InventoryGauge({ custId, topFamilies, isLoading }) {
  const queryClient = useQueryClient();
  const [addingItemId, setAddingItemId] = useState(null);
  const [justAddedId, setJustAddedId] = useState(null);

  // Pick the customer's top family by revenue
  const primaryFamily = useMemo(() => {
    if (!Array.isArray(topFamilies) || topFamilies.length === 0) return null;
    return topFamilies[0].family || null;
  }, [topFamilies]);

  // Query the catalog for that family. We pull a generous window so we can
  // pick the lowest-stock items that are still > 0 (skip out-of-stock since
  // those aren't actionable as reorders).
  const productsQuery = useQuery({
    queryKey: ['inventory-gauge', custId, primaryFamily],
    queryFn: () =>
      browseProducts({
        family: primaryFamily,
        in_stock_only: true,
        limit: 50
      }),
    enabled: Boolean(primaryFamily)
  });

  const addMutation = useMutation({
    mutationFn: ({ itemId }) => addToCart(custId, itemId, 1, 'manual'),
    onMutate: ({ itemId }) => setAddingItemId(itemId),
    onSuccess: (_d, vars) => {
      setJustAddedId(vars.itemId);
      setTimeout(() => setJustAddedId(null), 2000);
    },
    onSettled: () => {
      setAddingItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
      queryClient.invalidateQueries({ queryKey: ['cart', 'me'] });
    }
  });

  const lowStockItems = useMemo(() => {
    const items = (productsQuery.data && productsQuery.data.items) || [];
    return items
      .filter((it) => it.units_in_stock > 0 && it.units_in_stock < LOW_STOCK_THRESHOLD)
      .sort((a, b) => (a.units_in_stock || 0) - (b.units_in_stock || 0))
      .slice(0, MAX_VISIBLE);
  }, [productsQuery.data]);

  // Determine display state
  let body;
  if (isLoading || productsQuery.isLoading) {
    body = (
      <div className="h-[180px] flex items-center justify-center text-xs text-slate-400">
        Checking inventory...
      </div>
    );
  } else if (!primaryFamily) {
    body = (
      <div className="h-[180px] flex flex-col items-center justify-center text-xs text-slate-400">
        <Package size={20} className="text-slate-300 mb-2" />
        Order some products to start tracking inventory.
      </div>
    );
  } else if (lowStockItems.length === 0) {
    body = (
      <div className="h-[180px] flex flex-col items-center justify-center text-xs text-slate-400">
        <Check size={20} className="text-green-400 mb-2" />
        All <span className="font-semibold text-mck-navy mx-1">{primaryFamily}</span> items are well-stocked.
      </div>
    );
  } else {
    body = (
      <ul className="space-y-3">
        {lowStockItems.map((item) => (
          <GaugeRow
            key={item.item_id}
            item={item}
            adding={addingItemId === item.item_id ? 1 : 0}
            justAdded={justAddedId === item.item_id ? 1 : 0}
            onReorder={() => addMutation.mutate({ itemId: item.item_id })}
          />
        ))}
      </ul>
    );
  }

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <AlertTriangle size={14} className="text-mck-orange" />
          <h3 className="text-sm font-semibold text-mck-navy">Low stock alerts</h3>
        </div>
        {primaryFamily ? (
          <div className="text-[11px] text-slate-500 truncate max-w-[10rem]">
            in {primaryFamily}
          </div>
        ) : null}
      </div>
      <div className="px-5 pb-5">{body}</div>
    </Card>
  );
}

function GaugeRow({ item, adding, justAdded, onReorder }) {
  const stock = item.units_in_stock || 0;
  // Gauge fill - clamp to LOW_STOCK_THRESHOLD so the bar is meaningful.
  // Below 10: red. 10-19: orange. 20-29: yellow.
  const pct = Math.max(4, Math.min(100, (stock / LOW_STOCK_THRESHOLD) * 100));
  let barColor = 'bg-yellow-400';
  if (stock < 10) barColor = 'bg-red-500';
  else if (stock < 20) barColor = 'bg-mck-orange';

  return (
    <li>
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1 min-w-0">
          <div className="text-xs font-semibold text-mck-navy truncate" title={item.description}>
            {item.description || `Item ${item.item_id}`}
          </div>
          <div className="text-[11px] text-slate-500 mt-0.5 flex items-center gap-x-2 flex-wrap">
            <span className="font-mono">SKU {item.item_id}</span>
            {item.unit_price !== null && item.unit_price !== undefined ? (
              <>
                <span className="text-slate-300">|</span>
                <span>{formatCurrency(item.unit_price)}</span>
              </>
            ) : null}
          </div>
        </div>
        <button
          type="button"
          onClick={onReorder}
          disabled={adding === 1 || justAdded === 1}
          className={`inline-flex items-center gap-1 px-2.5 py-1 text-[11px] font-semibold rounded transition-colors flex-shrink-0 ${
            justAdded === 1
              ? 'bg-green-500 text-white'
              : 'bg-mck-blue text-white hover:bg-mck-blue-dark disabled:opacity-50 disabled:cursor-not-allowed'
          }`}
        >
          {adding === 1 ? (
            <>
              <Loader2 size={11} className="animate-spin" />
              Adding
            </>
          ) : justAdded === 1 ? (
            <>
              <Check size={11} />
              Added
            </>
          ) : (
            <>
              <RefreshCw size={11} />
              Reorder
            </>
          )}
        </button>
      </div>
      <div className="mt-1.5 flex items-center gap-2">
        <div className="flex-1 h-1.5 bg-slate-100 rounded-full overflow-hidden">
          <div
            className={`h-full ${barColor} transition-all`}
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="text-[11px] text-slate-500 tabular-nums flex-shrink-0">
          {formatNumber(stock)} left
        </span>
      </div>
    </li>
  );
}
