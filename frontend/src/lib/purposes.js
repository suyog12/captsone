import { Sparkles, RotateCcw, Plus, Replace, RefreshCcw, Tag } from 'lucide-react';

// Recommendation purposes

export const PURPOSES = {
  new_product: {
    label: 'New Product',
    description: 'Catalog expansion: a product the customer has never purchased.',
    color: 'bg-blue-100 text-blue-800 border-blue-200',
    icon: Sparkles
  },
  win_back: {
    label: 'Win-back',
    description: 'Re-engagement of a previously purchased item.',
    color: 'bg-orange-100 text-orange-800 border-orange-200',
    icon: RotateCcw
  },
  cross_sell: {
    label: 'Cross-sell',
    description: 'Complement to items the customer regularly orders.',
    color: 'bg-violet-100 text-violet-800 border-violet-200',
    icon: Plus
  },
  mckesson_substitute: {
    label: 'McKesson Substitute',
    description: 'McKesson Brand alternative to a competitor product.',
    color: 'bg-red-100 text-red-800 border-red-200',
    icon: Replace
  },
  replenishment: {
    label: 'Replenishment',
    description: 'Replenishment cycle for a recurring purchase.',
    color: 'bg-green-100 text-green-800 border-green-200',
    icon: RefreshCcw
  }
};

export function getPurpose(key) {
  if (!key) {
    return { label: 'General', description: '', color: 'bg-slate-100 text-slate-700 border-slate-200', icon: Tag };
  }
  return PURPOSES[key] || { label: humanize(key), description: '', color: 'bg-slate-100 text-slate-700 border-slate-200', icon: Tag };
}

export function listPurposes() {
  return Object.keys(PURPOSES).map((key) => ({ key, ...PURPOSES[key] }));
}

function humanize(s) {
  return s
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}
