// Formatting helpers

// API returns prices as strings (e.g. "12.99") so we normalize to a number first.
export function formatCurrency(value, fallback = '-') {
  if (value === null || value === undefined || value === '') return fallback;
  const n = typeof value === 'string' ? parseFloat(value) : value;
  if (Number.isNaN(n)) return fallback;
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(n);
}

export function formatNumber(value, fallback = '-') {
  if (value === null || value === undefined) return fallback;
  const n = typeof value === 'string' ? parseFloat(value) : value;
  if (Number.isNaN(n)) return fallback;
  return new Intl.NumberFormat('en-US').format(n);
}

export function formatPercent(value, digits = 1, fallback = '-') {
  if (value === null || value === undefined) return fallback;
  const n = typeof value === 'string' ? parseFloat(value) : value;
  if (Number.isNaN(n)) return fallback;
  // API returns peer_adoption_rate as a fraction (0.42), but conversion_rate_pct as already-percent.
  // Use formatPercentFraction for fractions and formatPercentValue for already-percent values.
  return `${(n * 100).toFixed(digits)}%`;
}

export function formatPercentValue(value, digits = 1, fallback = '-') {
  if (value === null || value === undefined) return fallback;
  const n = typeof value === 'string' ? parseFloat(value) : value;
  if (Number.isNaN(n)) return fallback;
  return `${n.toFixed(digits)}%`;
}

export function formatDate(value, fallback = '-') {
  if (!value) return fallback;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return fallback;
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

export function formatDateTime(value, fallback = '-') {
  if (!value) return fallback;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return fallback;
  return d.toLocaleString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
    hour: 'numeric',
    minute: '2-digit'
  });
}

export function relativeTime(value, fallback = '-') {
  if (!value) return fallback;
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return fallback;
  const diffMs = Date.now() - d.getTime();
  const sec = Math.round(diffMs / 1000);
  const min = Math.round(sec / 60);
  const hr = Math.round(min / 60);
  const day = Math.round(hr / 24);
  if (sec < 60) return 'just now';
  if (min < 60) return `${min}m ago`;
  if (hr < 24) return `${hr}h ago`;
  if (day < 30) return `${day}d ago`;
  return formatDate(value);
}

// Confidence tier returned by the API as 'high' | 'medium' | 'low'
export function confidenceFraction(tier) {
  if (tier === 'high') return 0.9;
  if (tier === 'medium') return 0.6;
  if (tier === 'low') return 0.3;
  return 0;
}

export function confidenceLabel(tier) {
  if (tier === 'high') return 'High';
  if (tier === 'medium') return 'Medium';
  if (tier === 'low') return 'Low';
  return 'Unknown';
}
