import { useRef, useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { ChevronLeft, ChevronRight, Plus, Loader2, Check, Award, Sparkles, ArrowRight } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import Card from '../ui/Card.jsx';
import SignalBadge from '../ui/SignalBadge.jsx';
import ProductImage from '../ui/ProductImage.jsx';
import { addToCart } from '../../api.js';
import { signalToCartSource } from '../../lib/signals.js';
import { formatCurrency } from '../../lib/format.js';

// Suggested for you carousel

// Compact horizontal scroller of the customer's top 6 recommendations.
// Lighter than RecommendationCard - just image, name, signal pill, price,
// and a one-click Add. Each Add passes the signal-derived cart source so
// conversion stats track per-signal.

export default function SuggestedCarousel({ recs = [], custId, onSeeAll }) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const scrollerRef = useRef(null);
  const [addingItemId, setAddingItemId] = useState(null);
  const [justAddedId, setJustAddedId] = useState(null);

  const addMutation = useMutation({
    mutationFn: ({ itemId, source }) => addToCart(custId, itemId, 1, source),
    onMutate: ({ itemId }) => setAddingItemId(itemId),
    onSuccess: (_d, vars) => {
      setJustAddedId(vars.itemId);
      setTimeout(() => setJustAddedId(null), 2000);
    },
    onSettled: () => {
      setAddingItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
      queryClient.invalidateQueries({ queryKey: ['cart', 'me'] });
      queryClient.invalidateQueries({ queryKey: ['cart-helper'] });
    }
  });

  function handleAdd(rec) {
    if (!custId) return;
    const source = signalToCartSource(rec.primary_signal);
    addMutation.mutate({ itemId: rec.item_id, source });
  }

  function scrollBy(delta) {
    if (!scrollerRef.current) return;
    scrollerRef.current.scrollBy({ left: delta, behavior: 'smooth' });
  }

  const visible = recs.slice(0, 6);

  function handleSeeAll() {
    if (onSeeAll) onSeeAll();
    else navigate('/customer/recommendations');
  }

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 flex items-center justify-between flex-wrap gap-2">
        <div className="flex items-center gap-2">
          <Sparkles size={14} className="text-mck-orange" />
          <h3 className="text-sm font-semibold text-mck-navy">Suggested for you</h3>
        </div>
        <div className="flex items-center gap-1.5">
          <button
            type="button"
            onClick={() => scrollBy(-300)}
            className="p-1.5 rounded hover:bg-slate-100 text-slate-500 hover:text-mck-navy"
            aria-label="Scroll left"
          >
            <ChevronLeft size={14} />
          </button>
          <button
            type="button"
            onClick={() => scrollBy(300)}
            className="p-1.5 rounded hover:bg-slate-100 text-slate-500 hover:text-mck-navy"
            aria-label="Scroll right"
          >
            <ChevronRight size={14} />
          </button>
          <button
            type="button"
            onClick={handleSeeAll}
            className="text-xs font-semibold text-mck-blue hover:text-mck-blue-dark inline-flex items-center gap-1 ml-1"
          >
            See all <ArrowRight size={12} />
          </button>
        </div>
      </div>

      {visible.length === 0 ? (
        <div className="px-5 pb-5 text-center text-xs text-slate-400 py-4">
          Recommendations will appear here as your purchase patterns build.
        </div>
      ) : (
        <div
          ref={scrollerRef}
          className="px-5 pb-5 flex items-stretch gap-3 overflow-x-auto scroll-smooth snap-x"
          style={{ scrollbarWidth: 'thin' }}
        >
          {visible.map((rec) => (
            <CarouselCard
              key={rec.item_id}
              rec={rec}
              adding={addingItemId === rec.item_id ? 1 : 0}
              justAdded={justAddedId === rec.item_id ? 1 : 0}
              onAdd={() => handleAdd(rec)}
            />
          ))}
        </div>
      )}
    </Card>
  );
}

function CarouselCard({ rec, adding, justAdded, onAdd }) {
  const stock = rec.units_in_stock;
  const stockOut = stock !== null && stock !== undefined && stock <= 0;
  const isMckesson = rec.is_mckesson_brand === true || rec.is_mckesson_brand === 1;

  return (
    <div className="snap-start flex-shrink-0 w-[14rem] bg-white border border-slate-200 rounded-lg overflow-hidden flex flex-col hover:border-mck-blue/40 transition-colors">
      <div className="aspect-square bg-slate-50 flex items-center justify-center p-3 relative">
        <ProductImage size="lg" item={rec} alt={rec.description} />
        {isMckesson ? (
          <div className="absolute top-1.5 left-1.5 inline-flex items-center gap-1 px-1.5 py-0.5 bg-mck-orange/10 text-mck-orange text-[9px] font-bold uppercase tracking-wider rounded-full border border-mck-orange/30">
            <Award size={9} />
            McKesson
          </div>
        ) : null}
      </div>

      <div className="p-3 flex-1 flex flex-col">
        <div className="flex-1 mb-2">
          <div className="text-xs font-semibold text-mck-navy line-clamp-2 leading-snug min-h-[2.5rem]">
            {rec.description || `Item ${rec.item_id}`}
          </div>
          <div className="mt-1.5">
            <SignalBadge signal={rec.primary_signal} size="xs" />
          </div>
        </div>

        <div className="flex items-end justify-between mb-2">
          <div className="text-sm font-bold text-mck-navy">
            {formatCurrency(rec.median_unit_price)}
          </div>
          <div className="text-[10px] text-slate-500">
            {stockOut ? (
              <span className="text-red-500">Out of stock</span>
            ) : stock !== null && stock !== undefined ? (
              `${stock} left`
            ) : null}
          </div>
        </div>

        <button
          type="button"
          onClick={onAdd}
          disabled={adding === 1 || stockOut || justAdded === 1}
          className={`w-full inline-flex items-center justify-center gap-1 px-2 py-1.5 text-[11px] font-semibold rounded transition-colors ${
            justAdded === 1
              ? 'bg-green-500 text-white'
              : stockOut
              ? 'bg-slate-100 text-slate-400 cursor-not-allowed'
              : 'bg-mck-blue text-white hover:bg-mck-blue-dark disabled:opacity-60'
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
          ) : stockOut ? (
            'Out of stock'
          ) : (
            <>
              <Plus size={11} />
              Add to cart
            </>
          )}
        </button>
      </div>
    </div>
  );
}
