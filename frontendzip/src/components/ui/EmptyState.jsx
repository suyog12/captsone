import { Inbox } from 'lucide-react';

// Empty state

export default function EmptyState({ icon: Icon = Inbox, title = 'Nothing to show', description, action, className = '' }) {
  return (
    <div className={`flex flex-col items-center justify-center text-center py-12 px-4 ${className}`}>
      <div className="text-slate-400 mb-3">
        <Icon size={36} strokeWidth={1.5} />
      </div>
      <div className="text-sm font-semibold text-slate-700">{title}</div>
      {description ? <div className="text-xs text-slate-500 mt-1 max-w-md">{description}</div> : null}
      {action ? <div className="mt-4">{action}</div> : null}
    </div>
  );
}
