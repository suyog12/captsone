// Segmented control

export default function SegmentedControl({ options, value, onChange, size = 'sm', className = '' }) {
  const sizeMap = {
    xs: 'text-[11px] px-2 py-0.5',
    sm: 'text-xs px-2.5 py-1',
    md: 'text-sm px-3 py-1.5'
  };
  const padding = sizeMap[size] || sizeMap.sm;
  return (
    <div className={`inline-flex items-center bg-slate-100 rounded-md p-0.5 ${className}`}>
      {options.map((opt) => {
        const active = opt.value === value;
        return (
          <button
            key={opt.value}
            type="button"
            onClick={() => onChange(opt.value)}
            className={`${padding} font-medium rounded transition-colors ${
              active ? 'bg-white text-mck-navy shadow-sm' : 'text-slate-500 hover:text-mck-navy'
            }`}
          >
            {opt.label}
          </button>
        );
      })}
    </div>
  );
}
