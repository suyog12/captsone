import { TrendingUp, TrendingDown, AlertOctagon, Sparkles, Activity } from 'lucide-react';

// Lifecycle status

export const LIFECYCLE = {
  stable_warm: {
    label: 'Stable',
    description: 'Healthy, recurring purchase pattern.',
    color: 'bg-green-100 text-green-800 border-green-200',
    dot: 'bg-lifecycle-stable_warm',
    icon: TrendingUp
  },
  declining_warm: {
    label: 'Declining',
    description: 'Order velocity slowing. At-risk account.',
    color: 'bg-yellow-100 text-yellow-800 border-yellow-200',
    dot: 'bg-lifecycle-declining_warm',
    icon: TrendingDown
  },
  churned_warm: {
    label: 'Churned',
    description: 'Inactive for an extended period. Win-back priority.',
    color: 'bg-red-100 text-red-800 border-red-200',
    dot: 'bg-lifecycle-churned_warm',
    icon: AlertOctagon
  },
  cold_start: {
    label: 'Cold Start',
    description: 'New account with limited purchase history.',
    color: 'bg-slate-100 text-slate-700 border-slate-200',
    dot: 'bg-lifecycle-cold_start',
    icon: Sparkles
  }
};

export function getLifecycle(key) {
  if (!key) {
    return { label: 'Unknown', description: '', color: 'bg-slate-100 text-slate-700 border-slate-200', dot: 'bg-slate-400', icon: Activity };
  }
  return LIFECYCLE[key] || { label: humanize(key), description: '', color: 'bg-slate-100 text-slate-700 border-slate-200', dot: 'bg-slate-400', icon: Activity };
}

export function listLifecycles() {
  return Object.keys(LIFECYCLE).map((key) => ({ key, ...LIFECYCLE[key] }));
}

function humanize(s) {
  return s
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}
