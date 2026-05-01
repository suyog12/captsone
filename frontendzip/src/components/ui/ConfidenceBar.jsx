import { confidenceFraction, confidenceLabel } from '../../lib/format.js';

// Confidence bar

const TIER_COLOR = {
  high: 'bg-green-500',
  medium: 'bg-amber-500',
  low: 'bg-slate-400'
};

const TIER_TEXT = {
  high: 'text-green-700',
  medium: 'text-amber-700',
  low: 'text-slate-500'
};

export default function ConfidenceBar({ tier, showLabel = 1, className = '' }) {
  const fraction = confidenceFraction(tier);
  const pct = Math.round(fraction * 100);
  const fill = TIER_COLOR[tier] || 'bg-slate-300';
  const txt = TIER_TEXT[tier] || 'text-slate-500';
  return (
    <div className={`flex items-center gap-2 ${className}`}>
      {showLabel === 1 ? (
        <span className={`text-xs font-medium ${txt} min-w-[3.5rem]`}>{confidenceLabel(tier)}</span>
      ) : null}
      <div className="flex-1 h-1.5 bg-slate-100 rounded-full overflow-hidden min-w-[60px]">
        <div className={`h-full rounded-full ${fill}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
}
