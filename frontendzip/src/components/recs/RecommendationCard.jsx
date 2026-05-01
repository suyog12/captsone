import { useState } from 'react';
import {
  ChevronDown,
  ChevronUp,
  ShoppingCart,
  Award,
  Package2,
  Tag,
  Users,
  Stethoscope,
  Plus,
  Layers,
  Check,
  X
} from 'lucide-react';
import SignalBadge from '../ui/SignalBadge.jsx';
import PurposeBadge from '../ui/PurposeBadge.jsx';
import ConfidenceBar from '../ui/ConfidenceBar.jsx';
import ProductImage from '../ui/ProductImage.jsx';
import PitchReason from './PitchReason.jsx';
import { signalToCartSource } from '../../lib/signals.js';
import { formatCurrency, formatPercent } from '../../lib/format.js';

// Recommendation card

// Renders a single recommendation card. Supports grouped recommendations
// (variantCount > 1 changes the action button label and shows a sizes
// badge). The Reject button is seller-only and surfaced via the
// canReject prop, which the parent sets based on user role.

export default function RecommendationCard({
  rec,
  variantCount = 1,
  onAddToCart,
  onReject,
  addingToCart = 0,
  justAdded = 0,
  canReject = 0,
  showRank = 1
}) {
  const [expanded, setExpanded] = useState(0);

  const isPB = rec.is_private_brand === true || rec.is_private_brand === 1;
  const isMckesson = rec.is_mckesson_brand === true || rec.is_mckesson_brand === 1;
  const stock = rec.units_in_stock;
  const stockLow = stock !== null && stock !== undefined && stock < 10;
  const stockOut = stock !== null && stock !== undefined && stock <= 0;
  const isMultiVariant = variantCount > 1;

  function handleAdd() {
    if (!onAddToCart) return;
    onAddToCart(rec, signalToCartSource(rec.primary_signal));
  }

  function handleReject() {
    if (!onReject) return;
    onReject(rec);
  }

  return (
    <div className="bg-white rounded-lg shadow-card border border-slate-200 overflow-hidden hover:shadow-card-hover transition-shadow">
      <div className="px-5 pt-4 pb-3">
        <div className="flex items-start gap-4">
          {showRank === 1 ? (
            <div className="flex-shrink-0 w-10 h-10 rounded-md bg-mck-navy text-white flex items-center justify-center">
              <div className="text-base font-bold leading-none">{rec.rank}</div>
            </div>
          ) : null}

          <ProductImage size="md" alt={rec.description || `Item ${rec.item_id}`} />

          <div className="flex-1 min-w-0">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0">
                <h3 className="text-sm font-semibold text-mck-navy leading-snug">
                  {rec.description || `Item ${rec.item_id}`}
                </h3>
                <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-2 flex-wrap">
                  {isMultiVariant ? (
                    <>
                      <span>{variantCount} SKUs in this family</span>
                      <span className="text-slate-300">|</span>
                    </>
                  ) : (
                    <>
                      <span>SKU {rec.item_id}</span>
                      <span className="text-slate-300">|</span>
                    </>
                  )}
                  {rec.family ? <span>{rec.family}</span> : null}
                  {rec.category ? (
                    <>
                      <span className="text-slate-300">|</span>
                      <span>{rec.category}</span>
                    </>
                  ) : null}
                </div>
              </div>

              <div className="flex-shrink-0 text-right">
                <div className="text-lg font-bold text-mck-navy leading-tight">
                  {formatCurrency(rec.median_unit_price)}
                </div>
                <StockPill stock={stock} stockLow={stockLow} stockOut={stockOut} />
              </div>
            </div>

            <div className="flex items-center gap-1.5 flex-wrap mt-2">
              <SignalBadge signal={rec.primary_signal} />
              {rec.rec_purpose ? <PurposeBadge purpose={rec.rec_purpose} /> : null}
              {isMultiVariant ? <SizesBadge count={variantCount} /> : null}
              {isMckesson ? <McKessonBrandBadge /> : null}
              {isPB ? <PrivateBrandBadge /> : null}
            </div>
          </div>
        </div>
      </div>

      {rec.pitch_reason ? (
        <div className="px-5 pb-3">
          <PitchReason text={rec.pitch_reason} />
        </div>
      ) : null}

      <div className="border-t border-slate-100 px-5 py-3 flex items-center justify-between gap-3 bg-slate-50/40">
        <div className="flex-1 min-w-0 max-w-[16rem]">
          <ConfidenceBar tier={rec.confidence_tier} />
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          <button
            type="button"
            onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
            className="text-xs font-medium text-slate-500 hover:text-mck-navy flex items-center gap-1 px-2 py-1.5 rounded hover:bg-slate-100"
          >
            {expanded === 1 ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
            Why
          </button>
          {/* Reject - seller-only feedback button */}
          {canReject === 1 && onReject ? (
            <button
              type="button"
              onClick={handleReject}
              className="text-xs font-medium text-slate-500 hover:text-red-600 flex items-center gap-1 px-2 py-1.5 rounded hover:bg-red-50"
              title="Tell us this rec is not useful"
            >
              <X size={13} />
              Reject
            </button>
          ) : null}
          {onAddToCart ? (
            <button
              type="button"
              onClick={handleAdd}
              disabled={addingToCart === 1 || stockOut || justAdded === 1}
              className={`text-xs font-semibold px-3 py-1.5 rounded flex items-center gap-1.5 transition-colors ${
                justAdded === 1
                  ? 'bg-green-500 text-white'
                  : stockOut
                  ? 'bg-slate-300 text-slate-500 cursor-not-allowed'
                  : 'text-white bg-mck-blue hover:bg-mck-blue-dark disabled:bg-slate-300 disabled:cursor-not-allowed'
              }`}
            >
              {addingToCart === 1 ? (
                <>Adding...</>
              ) : justAdded === 1 ? (
                <>
                  <Check size={13} />
                  Added
                </>
              ) : isMultiVariant ? (
                <>
                  <ShoppingCart size={13} />
                  Choose sizes
                </>
              ) : (
                <>
                  <Plus size={13} />
                  Add to cart
                </>
              )}
            </button>
          ) : null}
        </div>
      </div>

      {expanded === 1 ? (
        <div className="border-t border-slate-100 px-5 py-4 bg-slate-50/60">
          <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">
            Supporting evidence
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
            <Detail icon={Users} label="Peer adoption" value={formatPercent(rec.peer_adoption_rate)} />
            <Detail icon={Stethoscope} label="Specialty match" value={rec.specialty_match || '-'} />
            <Detail icon={Tag} label="Family" value={rec.family || '-'} />
            <Detail icon={Package2} label="Category" value={rec.category || '-'} />
            <Detail icon={ShoppingCart} label="Units in stock" value={stock !== null && stock !== undefined ? stock : '-'} />
            <Detail icon={Award} label="Confidence" value={(rec.confidence_tier || '-').toString()} />
          </div>
        </div>
      ) : null}
    </div>
  );
}

function Detail({ icon: Icon, label, value }) {
  return (
    <div className="flex items-start gap-2">
      <Icon size={14} className="text-slate-400 flex-shrink-0 mt-0.5" />
      <div className="min-w-0">
        <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">{label}</div>
        <div className="text-xs text-mck-navy font-medium truncate">{value}</div>
      </div>
    </div>
  );
}

function StockPill({ stock, stockLow, stockOut }) {
  if (stock === null || stock === undefined) return null;
  let cls = 'text-slate-500 bg-slate-100';
  let txt = `${stock} in stock`;
  if (stockOut) {
    cls = 'text-red-700 bg-red-50';
    txt = 'Out of stock';
  } else if (stockLow) {
    cls = 'text-amber-700 bg-amber-50';
    txt = `Only ${stock} left`;
  }
  return (
    <div className={`inline-block text-[10px] font-medium px-1.5 py-0.5 rounded mt-1 ${cls}`}>{txt}</div>
  );
}

function SizesBadge({ count }) {
  return (
    <span className="inline-flex items-center gap-1 text-xs font-semibold text-mck-navy bg-slate-100 border border-slate-200 px-2 py-0.5 rounded-full">
      <Layers size={12} />
      {count} sizes
    </span>
  );
}

function McKessonBrandBadge() {
  return (
    <span className="inline-flex items-center gap-1 text-xs font-semibold text-mck-orange bg-orange-50 border border-orange-200 px-2 py-0.5 rounded-full">
      <Award size={12} />
      McKesson Brand
    </span>
  );
}

function PrivateBrandBadge() {
  return (
    <span className="inline-flex items-center gap-1 text-xs font-semibold text-violet-700 bg-violet-50 border border-violet-200 px-2 py-0.5 rounded-full">
      <Award size={12} />
      Private Brand
    </span>
  );
}
