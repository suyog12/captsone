import Badge from './Badge.jsx';
import { getPurpose } from '../../lib/purposes.js';

// Purpose badge

export default function PurposeBadge({ purpose, size = 'sm', showIcon = 1, className = '' }) {
  const p = getPurpose(purpose);
  return (
    <Badge color={p.color} icon={showIcon === 1 ? p.icon : undefined} size={size} className={className}>
      {p.label}
    </Badge>
  );
}
