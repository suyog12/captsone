// Badge

export default function Badge({ children, color = 'bg-slate-100 text-slate-700 border-slate-200', dot, icon: Icon, size = 'sm', className = '' }) {
  const sizeMap = {
    xs: 'text-[10px] px-1.5 py-0.5',
    sm: 'text-xs px-2 py-0.5',
    md: 'text-sm px-2.5 py-1'
  };
  return (
    <span
      className={`inline-flex items-center gap-1 font-medium rounded-full border ${color} ${sizeMap[size] || sizeMap.sm} ${className}`}
    >
      {dot ? <span className={`w-1.5 h-1.5 rounded-full ${dot}`} /> : null}
      {Icon ? <Icon size={size === 'md' ? 14 : 12} className="flex-shrink-0" /> : null}
      <span className="whitespace-nowrap">{children}</span>
    </span>
  );
}
