import {
  Users,
  TrendingUp,
  ShoppingBasket,
  GitCompareArrows,
  RefreshCcw,
  RotateCcw,
  Package,
  ArrowRightLeft
} from 'lucide-react';

// Signal types

// Backend produces 8 signal types. Design template originally specified 5
// (peer_gap, replenishment, co_occurrence, private_brand, item_similarity).
// We map design's "co_occurrence" -> "cart_complement" and design's
// "private_brand" -> "private_brand_upgrade", and add the 3 missing
// signals (popularity, lapsed_recovery, medline_conversion) using the same
// SignalBadge pattern with distinct colors.

export const SIGNALS = {
  peer_gap: {
    label: 'Peer Gap',
    description: 'Bought by similar customers but missing from your catalog.',
    color: 'bg-blue-100 text-blue-800 border-blue-200',
    dot: 'bg-signal-peer_gap',
    icon: Users
  },
  popularity: {
    label: 'Popular Pick',
    description: 'A popular product among peers in your market.',
    color: 'bg-sky-100 text-sky-800 border-sky-200',
    dot: 'bg-signal-popularity',
    icon: TrendingUp
  },
  cart_complement: {
    label: 'Cart Complement',
    description: 'Frequently bought together with items you already buy.',
    color: 'bg-violet-100 text-violet-800 border-violet-200',
    dot: 'bg-signal-cart_complement',
    icon: ShoppingBasket
  },
  item_similarity: {
    label: 'Similar Item',
    description: 'Similar to products you have purchased before.',
    color: 'bg-teal-100 text-teal-800 border-teal-200',
    dot: 'bg-signal-item_similarity',
    icon: GitCompareArrows
  },
  replenishment: {
    label: 'Replenishment',
    description: 'Reorder window approaching based on your purchase cadence.',
    color: 'bg-green-100 text-green-800 border-green-200',
    dot: 'bg-signal-replenishment',
    icon: RefreshCcw
  },
  lapsed_recovery: {
    label: 'Lapsed Recovery',
    description: 'You bought this before but not in 6+ months. Win-back opportunity.',
    color: 'bg-orange-100 text-orange-800 border-orange-200',
    dot: 'bg-signal-lapsed_recovery',
    icon: RotateCcw
  },
  private_brand_upgrade: {
    label: 'Private Brand',
    description: 'McKesson Brand equivalent of a national brand product.',
    color: 'bg-red-100 text-red-800 border-red-200',
    dot: 'bg-signal-private_brand_upgrade',
    icon: Package
  },
  medline_conversion: {
    label: 'Medline Conversion',
    description: 'McKesson alternative to a Medline product in your cart.',
    color: 'bg-purple-100 text-purple-800 border-purple-200',
    dot: 'bg-signal-medline_conversion',
    icon: ArrowRightLeft
  }
};

// Cart-helper sources from POST /recommendations/cart-helper. The backend
// can also tag complements with finer-grained sources like
// cart_complement_high_lift; we treat anything cart_complement_* as the same
// SignalBadge but show the suffix in the pitch reason text.
const CART_HELPER_SOURCE_TO_SIGNAL = {
  cart_complement_high_lift: 'cart_complement',
  private_brand_substitute: 'private_brand_upgrade',
  medline_to_mckesson: 'medline_conversion',
  replenishment_due: 'replenishment',
  peer_gap_complement: 'peer_gap'
};

// Map a recommendation primary_signal to the cart_items.source value the
// backend's CHECK constraint accepts. The backend enum lives in
// backend/models/cart_item.py and these values must stay in sync with it.
// IMPORTANT: hardcoding 'recommendation' for every signal (the previous
// behavior) violates the CHECK constraint and breaks conversion tracking.
const SIGNAL_TO_CART_SOURCE = {
  peer_gap: 'recommendation_peer_gap',
  popularity: 'recommendation_popularity',
  cart_complement: 'recommendation_cart_complement',
  item_similarity: 'recommendation_item_similarity',
  replenishment: 'recommendation_replenishment',
  lapsed_recovery: 'recommendation_lapsed',
  private_brand_upgrade: 'recommendation_pb_upgrade',
  medline_conversion: 'recommendation_medline_conversion'
};

export function signalToCartSource(signalKey) {
  if (!signalKey) return 'manual';
  if (SIGNAL_TO_CART_SOURCE[signalKey]) return SIGNAL_TO_CART_SOURCE[signalKey];
  // Cart-helper variants like cart_complement_high_lift map to their parent
  const parent = CART_HELPER_SOURCE_TO_SIGNAL[signalKey];
  if (parent && SIGNAL_TO_CART_SOURCE[parent]) return SIGNAL_TO_CART_SOURCE[parent];
  // Unknown signal -> default to manual to avoid CHECK constraint violation
  return 'manual';
}

export function getSignal(signalKey) {
  if (!signalKey) {
    return { label: 'Recommendation', description: '', color: 'bg-slate-100 text-slate-700 border-slate-200', dot: 'bg-slate-400', icon: TrendingUp };
  }
  const direct = SIGNALS[signalKey];
  if (direct) return direct;
  const mapped = CART_HELPER_SOURCE_TO_SIGNAL[signalKey];
  if (mapped) return SIGNALS[mapped];
  // Fallback for unknown signal codes
  return { label: humanize(signalKey), description: '', color: 'bg-slate-100 text-slate-700 border-slate-200', dot: 'bg-slate-400', icon: TrendingUp };
}

export function listSignals() {
  return Object.keys(SIGNALS).map((key) => ({ key, ...SIGNALS[key] }));
}

function humanize(s) {
  return s
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}
