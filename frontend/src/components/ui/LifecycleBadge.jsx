import Badge from './Badge.jsx';
import { getLifecycle } from '../../lib/lifecycle.js';

// Lifecycle badge

export default function LifecycleBadge({ status, size = 'sm', showIcon = 1, className = '' }) {
  const l = getLifecycle(status);
  return (
    <Badge color={l.color} icon={showIcon === 1 ? l.icon : undefined} size={size} className={className}>
      {l.label}
    </Badge>
  );
}
