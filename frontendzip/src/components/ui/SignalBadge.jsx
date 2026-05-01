import Badge from './Badge.jsx';
import { getSignal } from '../../lib/signals.js';

// Signal badge

export default function SignalBadge({ signal, size = 'sm', showIcon = 1, className = '' }) {
  const s = getSignal(signal);
  return (
    <Badge color={s.color} icon={showIcon === 1 ? s.icon : undefined} size={size} className={className}>
      {s.label}
    </Badge>
  );
}
