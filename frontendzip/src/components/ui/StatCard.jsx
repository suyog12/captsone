// Stat card

export default function StatCard({ label, value, hint, icon: Icon, accent = 'mck-blue', className = '' }) {
  // accent maps to a small left border stripe color
  const accentMap = {
    'mck-blue': 'border-l-mck-blue',
    'mck-orange': 'border-l-mck-orange',
    'mck-navy': 'border-l-mck-navy',
    green: 'border-l-green-500',
    red: 'border-l-red-500',
    slate: 'border-l-slate-400'
  };
  const stripe = accentMap[accent] || accentMap['mck-blue'];
  return (
    <div className={`bg-white rounded-lg shadow-card border border-slate-200 border-l-4 ${stripe} px-5 py-4 ${className}`}>
      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider">{label}</div>
          <div className="text-2xl font-bold text-mck-navy mt-1 leading-tight truncate">{value}</div>
          {hint ? <div className="text-xs text-slate-500 mt-1">{hint}</div> : null}
        </div>
        {Icon ? (
          <div className="ml-3 text-mck-blue flex-shrink-0">
            <Icon size={24} strokeWidth={1.75} />
          </div>
        ) : null}
      </div>
    </div>
  );
}
