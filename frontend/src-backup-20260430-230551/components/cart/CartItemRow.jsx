import { useState } from 'react';
import { Plus, Minus, Trash2, Loader2 } from 'lucide-react';
import ProductImage from '../ui/ProductImage.jsx';
import { formatCurrency } from '../../lib/format.js';

// Cart item row

export default function CartItemRow({ item, onUpdateQuantity, onRemove, readOnly = 0, busy = 0 }) {
  const [localQty, setLocalQty] = useState(item.quantity);

  function bump(delta) {
    const next = Math.max(1, (localQty || 1) + delta);
    setLocalQty(next);
    if (onUpdateQuantity) onUpdateQuantity(item.cart_item_id, next);
  }

  return (
    <div className="px-5 py-4 flex items-start gap-4 hover:bg-slate-50/40">
      <ProductImage size="sm" alt={item.description || `Item ${item.item_id}`} />

      <div className="flex-1 min-w-0">
        <div className="text-sm font-semibold text-mck-navy truncate">{item.description || `Item ${item.item_id}`}</div>
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
              <span className="text-mck-orange font-medium">via {prettyLabel(item.source)}</span>
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
  );
}

function prettyLabel(s) {
  if (!s) return '';
  return s.split('_').map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
}
