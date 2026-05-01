import { useState } from 'react';
import { Plus, Minus, Trash2, Loader2, Check, X } from 'lucide-react';
import ProductImage from '../ui/ProductImage.jsx';
import { formatCurrency } from '../../lib/format.js';

// Cart item row

// Modes:
//   readOnly=1            -> qty + total only (admin viewing)
//   readOnly=0, sellerMode=0 -> qty stepper + remove (customer)
//   readOnly=0, sellerMode=1 -> qty stepper + remove + Mark Sold + Mark Not Sold

export default function CartItemRow({
  item,
  onUpdateQuantity,
  onRemove,
  onMarkSold,
  onMarkNotSold,
  readOnly = 0,
  sellerMode = 0,
  busy = 0
}) {
  const [localQty, setLocalQty] = useState(item.quantity);

  function bump(delta) {
    const next = Math.max(1, (localQty || 1) + delta);
    setLocalQty(next);
    if (onUpdateQuantity) onUpdateQuantity(item.cart_item_id, next);
  }

  return (
    <div className="px-5 py-4 hover:bg-slate-50/40">
      <div className="flex items-start gap-4">
        <ProductImage size="sm" alt={item.description || `Item ${item.item_id}`} />

        <div className="flex-1 min-w-0">
          <div className="text-sm font-semibold text-mck-navy truncate">
            {item.description || `Item ${item.item_id}`}
          </div>
          <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-x-2 gap-y-0.5 flex-wrap">
            <span>SKU {item.item_id}</span>
            {item.family ? (
              <>
                <span className="text-slate-300">|</span>
                <span>{item.family}</span>
              </>
            ) : null}
            {item.source && item.source !== 'manual' ? (
              <>
                <span className="text-slate-300">|</span>
                <span className="text-mck-orange font-medium">via {prettySource(item.source)}</span>
              </>
            ) : null}
          </div>
          {item.added_by_username ? (
            <div className="text-[10px] text-slate-400 mt-1">
              Added by {item.added_by_username} ({item.added_by_role})
            </div>
          ) : null}
        </div>

        <div className="flex items-center gap-3">
          {readOnly === 1 ? (
            <div className="text-sm text-slate-600 px-2.5 py-1 bg-slate-100 rounded">Qty {item.quantity}</div>
          ) : (
            <div className="inline-flex items-center bg-white border border-slate-200 rounded-md overflow-hidden">
              <button
                type="button"
                onClick={() => bump(-1)}
                disabled={busy === 1 || localQty <= 1}
                className="px-2 py-1.5 text-slate-500 hover:text-mck-navy hover:bg-slate-50 disabled:text-slate-300 disabled:cursor-not-allowed"
              >
                <Minus size={12} />
              </button>
              <div className="px-3 text-sm font-semibold text-mck-navy min-w-[2rem] text-center">{localQty}</div>
              <button
                type="button"
                onClick={() => bump(1)}
                disabled={busy === 1}
                className="px-2 py-1.5 text-slate-500 hover:text-mck-navy hover:bg-slate-50 disabled:text-slate-300 disabled:cursor-not-allowed"
              >
                <Plus size={12} />
              </button>
            </div>
          )}

          <div className="text-right min-w-[5rem]">
            <div className="text-sm font-bold text-mck-navy">{formatCurrency(item.line_total)}</div>
            <div className="text-[10px] text-slate-500">
              {formatCurrency(item.unit_price_at_add)} each
            </div>
          </div>

          {readOnly === 1 ? null : (
            <button
              type="button"
              onClick={() => onRemove && onRemove(item.cart_item_id)}
              disabled={busy === 1}
              className="text-slate-400 hover:text-red-600 p-1.5 rounded hover:bg-red-50 disabled:text-slate-300 disabled:cursor-not-allowed"
              title="Remove from cart"
            >
              {busy === 1 ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
            </button>
          )}
        </div>
      </div>

      {/* Seller closing workflow: per-line Mark Sold / Mark Not Sold */}
      {sellerMode === 1 && readOnly === 0 ? (
        <div className="mt-3 pl-[3.25rem] flex items-center gap-2 flex-wrap">
          <button
            type="button"
            onClick={() => onMarkSold && onMarkSold(item.cart_item_id)}
            disabled={busy === 1}
            className="inline-flex items-center gap-1 px-2.5 py-1 text-[11px] font-semibold rounded border border-green-200 bg-green-50 text-green-800 hover:bg-green-100 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {busy === 1 ? <Loader2 size={11} className="animate-spin" /> : <Check size={11} />}
            Mark Sold
          </button>
          <button
            type="button"
            onClick={() => onMarkNotSold && onMarkNotSold(item.cart_item_id)}
            disabled={busy === 1}
            className="inline-flex items-center gap-1 px-2.5 py-1 text-[11px] font-semibold rounded border border-slate-200 bg-white text-slate-600 hover:bg-slate-50 disabled:opacity-50 disabled:cursor-not-allowed"
          >
            <X size={11} />
            Mark Not Sold
          </button>
          <span className="text-[10px] text-slate-400 italic">
            Closes the cart line. Mark Sold also writes to purchase history.
          </span>
        </div>
      ) : null}
    </div>
  );
}

// Prettify cart_items.source values for display. The backend enum uses
// underscore-prefixed strings like 'recommendation_peer_gap'; we strip the
// 'recommendation_' prefix and title-case the rest so users see "Peer Gap"
// rather than "Recommendation_peer_gap".
function prettySource(s) {
  if (!s) return '';
  let cleaned = s;
  if (cleaned.startsWith('recommendation_')) {
    cleaned = cleaned.slice('recommendation_'.length);
  }
  // Special-case the abbreviated source names so they read naturally
  if (cleaned === 'pb_upgrade') return 'Private Brand';
  if (cleaned === 'lapsed') return 'Lapsed Recovery';
  if (cleaned === 'medline_conversion') return 'Medline Conversion';
  if (cleaned === 'cart_complement') return 'Cart Complement';
  if (cleaned === 'peer_gap') return 'Peer Gap';
  if (cleaned === 'item_similarity') return 'Similar Item';
  return cleaned
    .split('_')
    .map((w) => (w.length === 0 ? w : w.charAt(0).toUpperCase() + w.slice(1)))
    .join(' ');
}
