// Card

export default function Card({ children, className = '', padding = 'md', as: Tag = 'div', ...rest }) {
  const padMap = {
    none: '',
    sm: 'p-3',
    md: 'p-5',
    lg: 'p-6'
  };
  return (
    <Tag
      className={`bg-white rounded-lg shadow-card border border-slate-200 ${padMap[padding] || padMap.md} ${className}`}
      {...rest}
    >
      {children}
    </Tag>
  );
}

export function CardHeader({ title, subtitle, action, className = '' }) {
  return (
    <div className={`flex items-start justify-between mb-4 ${className}`}>
      <div>
        {title ? <h2 className="text-base font-semibold text-mck-navy">{title}</h2> : null}
        {subtitle ? <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p> : null}
      </div>
      {action ? <div>{action}</div> : null}
    </div>
  );
}
