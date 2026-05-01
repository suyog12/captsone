import { Loader2 } from 'lucide-react';

// Spinner

export default function Spinner({ size = 'md', className = '', label }) {
  const sizeMap = {
    sm: 14,
    md: 20,
    lg: 32
  };
  const px = sizeMap[size] || sizeMap.md;
  return (
    <div className={`flex items-center justify-center gap-2 text-slate-500 ${className}`}>
      <Loader2 size={px} className="animate-spin" />
      {label ? <span className="text-sm">{label}</span> : null}
    </div>
  );
}

export function FullPanelSpinner({ label = 'Loading' }) {
  return (
    <div className="flex items-center justify-center py-16">
      <Spinner size="lg" label={label} />
    </div>
  );
}
