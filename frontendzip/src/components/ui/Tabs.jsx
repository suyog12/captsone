// Tabs

export default function Tabs({ tabs, value, onChange, className = '' }) {
  return (
    <div className={`border-b border-slate-200 ${className}`}>
      <div className="flex gap-1 -mb-px">
        {tabs.map((t) => {
          const active = t.value === value;
          return (
            <button
              key={t.value}
              type="button"
              onClick={() => onChange(t.value)}
              className={`px-4 py-2.5 text-sm font-medium border-b-2 transition-colors flex items-center gap-2 ${
                active
                  ? 'border-mck-blue text-mck-blue'
                  : 'border-transparent text-slate-500 hover:text-mck-navy hover:border-slate-300'
              }`}
            >
              {t.icon ? <t.icon size={14} /> : null}
              <span>{t.label}</span>
              {t.count !== undefined && t.count !== null ? (
                <span
                  className={`ml-1 inline-flex items-center justify-center min-w-[1.25rem] px-1.5 py-0.5 rounded-full text-[10px] font-semibold ${
                    active ? 'bg-mck-blue/10 text-mck-blue' : 'bg-slate-100 text-slate-600'
                  }`}
                >
                  {t.count}
                </span>
              ) : null}
            </button>
          );
        })}
      </div>
    </div>
  );
}
