import { useState } from 'react';
import { ChevronDown, ChevronUp } from 'lucide-react';

// Expandable

// Wraps a list of children. Shows the first `initial` items by default
// and reveals the rest when the user clicks "Show more". Used anywhere
// we want to keep a long list compact at first glance.

export default function Expandable({
  children,
  initial = 10,
  showMoreLabel = 'Show more',
  showLessLabel = 'Show less',
  className = '',
  // when 1, the toggle button gets a small top divider line for table-like contexts
  divider = 0
}) {
  const [expanded, setExpanded] = useState(0);
  const items = Array.isArray(children) ? children : [children];
  const total = items.length;
  const hidden = total - initial;

  if (total <= initial) {
    return <div className={className}>{items}</div>;
  }

  const visible = expanded === 1 ? items : items.slice(0, initial);

  return (
    <div className={className}>
      {visible}
      <div className={divider === 1 ? 'border-t border-slate-100' : ''}>
        <button
          type="button"
          onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
          className="w-full px-5 py-2.5 text-xs font-semibold text-mck-blue hover:text-mck-blue-dark hover:bg-mck-sky/40 transition-colors flex items-center justify-center gap-1.5"
        >
          {expanded === 1 ? (
            <>
              <ChevronUp size={14} />
              {showLessLabel}
            </>
          ) : (
            <>
              <ChevronDown size={14} />
              {showMoreLabel} ({hidden} more)
            </>
          )}
        </button>
      </div>
    </div>
  );
}
