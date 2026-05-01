import { useState, useEffect, useRef } from 'react';
import { X, Plus, Minus, ShoppingCart, Loader2, Check, AlertCircle, Award } from 'lucide-react';
import ProductImage from '../ui/ProductImage.jsx';
import { variantLabel } from '../../lib/variantStem.js';
import { formatCurrency, formatNumber } from '../../lib/format.js';

// Variant picker modal

// Opens when the user clicks Add to Cart on a grouped product card. Shows
// each variant (SKU) as a row with its own qty stepper. Clicking Add All
// fires one addToCart call per row with quantity > 0, so each size becomes
// its own cart line.

export default function VariantPickerModal({
  open,
  onClose,
  group,
  custId,
  source = 'manual',
  onAddVariant
}) {
  const [quantities, setQuantities] = useState({});
  const [submitting, setSubmitting] = useState(0);
  const [errors, setErrors] = useState({});
  const [completed, setCompleted] = useState({});
  const dialogRef = useRef(null);

  // Reset state when group changes / modal reopens
  useEffect(() => {
    if (open && group) {
      const initial = {};
      // Default qty 0 for all variants - user must explicitly pick
      for (let i = 0; i < group.variants.length; i = i + 1) {
        initial[group.variants[i].item_id] = 0;
      }
      setQuantities(initial);
      setErrors({});
      setCompleted({});
      setSubmitting(0);
    }
  }, [open, group]);

  // Esc to close
  useEffect(() => {
    if (!open) return;
    function onKey(e) {
      if (e.key === 'Escape' && submitting === 0) onClose();
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, submitting, onClose]);

  // Focus trap entry: focus dialog when opened
  useEffect(() => {
    if (open && dialogRef.current) {
      dialogRef.current.focus();
    }
  }, [open]);

  if (!open || !group) return null;

  const rep = group.representative;
  const totalQty = Object.values(quantities).reduce((s, q) => s + (q || 0), 0);
  const totalLines = Object.values(quantities).filter((q) => (q || 0) > 0).length;
  const totalCost = group.variants.reduce((s, v) => {
    const q = quantities[v.item_id] || 0;
    const price = parseFloat(v.unit_price) || 0;
    return s + (q * price);
  }, 0);

  function bump(itemId, delta, maxStock) {
    setQuantities((prev) => {
      const cur = prev[itemId] || 0;
      let next = cur + delta;
      if (next < 0) next = 0;
      if (typeof maxStock === 'number' && maxStock > 0 && next > maxStock) next = maxStock;
      return { ...prev, [itemId]: next };
    });
  }

  function setExact(itemId, value, maxStock) {
    const n = parseInt(value, 10);
    let v = Number.isNaN(n) ? 0 : n;
    if (v < 0) v = 0;
    if (typeof maxStock === 'number' && maxStock > 0 && v > maxStock) v = maxStock;
    setQuantities((prev) => ({ ...prev, [itemId]: v }));
  }

  async function handleAddAll() {
    if (totalLines === 0) return;
    setSubmitting(1);
    setErrors({});
    const newCompleted = {};
    const newErrors = {};

    // Fire each line sequentially. We could parallelize but sequential is
    // gentler on the API and more predictable for error handling.
    for (let i = 0; i < group.variants.length; i = i + 1) {
      const v = group.variants[i];
      const qty = quantities[v.item_id] || 0;
      if (qty <= 0) continue;
      try {
        await onAddVariant(v.item_id, qty, source);
        newCompleted[v.item_id] = 1;
      } catch (err) {
        const msg =
          (err && err.response && err.response.data && err.response.data.detail) ||
          (err && err.message) ||
          'Failed to add';
        newErrors[v.item_id] = typeof msg === 'string' ? msg : 'Failed to add';
      }
      // Update progressively so user sees rows complete as they go
      setCompleted({ ...newCompleted });
      setErrors({ ...newErrors });
    }

    setSubmitting(0);

    // If everything succeeded, close after a short beat so user sees confirmation
    if (Object.keys(newErrors).length === 0) {
      setTimeout(() => onClose(), 700);
    }
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="variant-picker-title"
    >
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-mck-navy/60 backdrop-blur-sm"
        onClick={() => submitting === 0 && onClose()}
      />

      {/* Dialog */}
      <div
        ref={dialogRef}
        tabIndex={-1}
        className="relative bg-white rounded-xl shadow-2xl border border-slate-200 w-full max-w-2xl max-h-[90vh] flex flex-col overflow-hidden outline-none"
      >
        {/* Header */}
        <div className="px-5 py-4 border-b border-slate-200 flex items-start gap-3">
          <ProductImage size="md" item={rep} alt={rep.description} />
          <div className="flex-1 min-w-0">
            <h2 id="variant-picker-title" className="text-base font-semibold text-mck-navy leading-tight">
              {prettyStem(group.key, rep.description)}
            </h2>
            <div className="text-xs text-slate-500 mt-1 flex items-center gap-x-2 flex-wrap">
              {rep.family ? <span>{rep.family}</span> : null}
              {rep.category ? (
                <>
                  <span className="text-slate-300">|</span>
                  <span>{rep.category}</span>
                </>
              ) : null}
              <span className="text-slate-300">|</span>
              <span>{group.variantCount} {group.variantCount === 1 ? 'size' : 'sizes'}</span>
              {rep.is_private_brand ? (
                <>
                  <span className="text-slate-300">|</span>
                  <span className="inline-flex items-center gap-1 text-mck-orange font-semibold">
                    <Award size={11} /> McKesson Brand
                  </span>
                </>
              ) : null}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            disabled={submitting === 1}
            className="text-slate-400 hover:text-mck-navy p-1.5 rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed flex-shrink-0"
            aria-label="Close"
          >
            <X size={18} />
          </button>
        </div>

        {/* Variant rows (scrollable) */}
        <div className="flex-1 overflow-y-auto divide-y divide-slate-100">
          {group.variants.map((v) => (
            <VariantRow
              key={v.item_id}
              variant={v}
              quantity={quantities[v.item_id] || 0}
              onBump={(delta) => bump(v.item_id, delta, v.units_in_stock)}
              onSet={(val) => setExact(v.item_id, val, v.units_in_stock)}
              completed={completed[v.item_id] === 1}
              error={errors[v.item_id]}
              disabled={submitting === 1}
            />
          ))}
        </div>

        {/* Footer */}
        <div className="px-5 py-4 border-t border-slate-200 bg-slate-50 flex items-center justify-between gap-3 flex-wrap">
          <div className="text-xs text-slate-600">
            {totalLines === 0 ? (
              <span className="italic text-slate-400">Pick quantities to begin</span>
            ) : (
              <>
                <span className="font-semibold text-mck-navy">{totalLines}</span> {totalLines === 1 ? 'line' : 'lines'} &middot;{' '}
                <span className="font-semibold text-mck-navy">{formatNumber(totalQty)}</span> units &middot;{' '}
                <span className="font-semibold text-mck-navy">{formatCurrency(totalCost)}</span>
              </>
            )}
          </div>
          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={onClose}
              disabled={submitting === 1}
              className="px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-100 rounded disabled:opacity-40"
            >
              Cancel
            </button>
            <button
              type="button"
              onClick={handleAddAll}
              disabled={!custId || totalLines === 0 || submitting === 1}
              className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded transition-colors bg-mck-blue text-white hover:bg-mck-blue-dark disabled:bg-slate-300 disabled:cursor-not-allowed"
            >
              {submitting === 1 ? (
                <>
                  <Loader2 size={12} className="animate-spin" />
                  Adding {totalLines}...
                </>
              ) : (
                <>
                  <ShoppingCart size={12} />
                  Add All ({totalLines})
                </>
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function VariantRow({ variant, quantity, onBump, onSet, completed, error, disabled }) {
  const inStock = variant.units_in_stock > 0;
  const lowStock = inStock && variant.units_in_stock < 20;
  const stockOut = !inStock;
  const label = variantLabel(variant.description || '') || variant.description || `Item ${variant.item_id}`;

  return (
    <div className={`px-5 py-3 flex items-center gap-3 ${completed ? 'bg-green-50/40' : ''}`}>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-semibold text-mck-navy">{label}</span>
          {completed ? (
            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-green-700 bg-green-100 border border-green-200 px-1.5 py-0.5 rounded">
              <Check size={10} /> Added
            </span>
          ) : null}
        </div>
        <div className="text-[11px] text-slate-500 mt-0.5 flex items-center gap-x-2 flex-wrap">
          <span className="font-mono">SKU {variant.item_id}</span>
          <span className="text-slate-300">|</span>
          <span className="text-mck-navy font-semibold">
            {variant.unit_price !== null && variant.unit_price !== undefined
              ? formatCurrency(variant.unit_price)
              : '-'}
          </span>
          <span className="text-slate-300">|</span>
          <span className={stockOut ? 'text-red-600' : lowStock ? 'text-mck-orange' : 'text-green-600'}>
            {stockOut
              ? 'Out of stock'
              : `${formatNumber(variant.units_in_stock)} in stock${lowStock ? ' (low)' : ''}`}
          </span>
        </div>
        {error ? (
          <div className="mt-1 inline-flex items-center gap-1 text-[11px] text-red-600">
            <AlertCircle size={11} /> {error}
          </div>
        ) : null}
      </div>

      <div className="flex items-center gap-1.5 flex-shrink-0">
        <button
          type="button"
          onClick={() => onBump(-1)}
          disabled={disabled || quantity <= 0 || stockOut || completed}
          className="p-1.5 rounded border border-slate-200 text-slate-500 hover:bg-slate-50 disabled:opacity-30 disabled:cursor-not-allowed"
          aria-label="Decrease quantity"
        >
          <Minus size={12} />
        </button>
        <input
          type="number"
          min="0"
          max={variant.units_in_stock || undefined}
          value={quantity}
          onChange={(e) => onSet(e.target.value)}
          disabled={disabled || stockOut || completed}
          className="w-14 text-center text-sm font-semibold text-mck-navy border border-slate-200 rounded py-1 focus:border-mck-blue focus:ring-1 focus:ring-mck-blue/20 outline-none disabled:bg-slate-50 disabled:text-slate-400"
        />
        <button
          type="button"
          onClick={() => onBump(1)}
          disabled={disabled || stockOut || completed || (variant.units_in_stock !== null && variant.units_in_stock !== undefined && quantity >= variant.units_in_stock)}
          className="p-1.5 rounded border border-slate-200 text-slate-500 hover:bg-slate-50 disabled:opacity-30 disabled:cursor-not-allowed"
          aria-label="Increase quantity"
        >
          <Plus size={12} />
        </button>
      </div>
    </div>
  );
}

// Capitalize the stem for display (the key includes lowercase + family prefix).
// We prefer the representative's description with the variant token stripped
// so the casing reads naturally.
function prettyStem(key, fallbackDesc) {
  // The key is "family|category|pb|stem" - extract the stem part
  const parts = (key || '').split('|');
  const stem = parts.length >= 4 ? parts[3] : '';
  if (!stem) return fallbackDesc || 'Product';
  // Title-case the stem
  return stem
    .split(' ')
    .map((w) => (w.length === 0 ? w : w.charAt(0).toUpperCase() + w.slice(1)))
    .join(' ');
}
