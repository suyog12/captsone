import { useMemo } from 'react';
import { BarChart3 } from 'lucide-react';
import Card from '../ui/Card.jsx';
import { formatCurrency } from '../../lib/format.js';

// Category mix bar chart

// Renders a horizontal bar chart of the customer's product families by
// revenue. Bars are sorted descending by share, top family is highlighted,
// and any revenue beyond the top families is rolled into a gray "Other"
// bar so the displayed total equals 100%.
//
// Why bars instead of a donut: family names are long (e.g. "Equipment &
// Equip Disposables") and donuts force truncation. With 6+ categories the
// donut also gets visually cluttered while bars stay scannable. And
// because top_families is capped server-side, donut slices don't sum to
// 100% which is misleading - the bar chart's explicit "Other" row makes
// the missing share visible.
//
// Expects a list shaped like CustomerTopFamilyRow:
//   { family, revenue, quantity_sold, order_count, pct_of_total_revenue }
// pct_of_total_revenue is computed against the customer's true total, so
// the sum of all returned families' percentages can be < 100. We use this
// to derive the "Other" bar.

const PALETTE = [
  '#1B6BBE', // mck-blue (top family)
  '#F4821F', // mck-orange
  '#16A34A', // green
  '#7C3AED', // violet
  '#DC2626', // red
  '#0EA5E9', // sky
  '#EAB308', // yellow
  '#0F766E'  // teal
];

const OTHER_COLOR = '#94A3B8'; // slate-400

export default function CategoryDonut({ topFamilies, isLoading }) {
  const { rows, totalRevenue } = useMemo(() => buildRows(topFamilies), [topFamilies]);

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 flex items-center justify-between gap-3 flex-wrap">
        <div className="flex items-center gap-2">
          <BarChart3 size={14} className="text-mck-blue" />
          <h3 className="text-sm font-semibold text-mck-navy">Category mix</h3>
        </div>
        {totalRevenue > 0 ? (
          <div className="text-[11px] text-slate-500">
            Total <span className="font-semibold text-mck-navy">{formatCurrency(totalRevenue)}</span>
          </div>
        ) : null}
      </div>
      <div className="px-5 pb-5">
        {isLoading ? (
          <div className="h-[240px] flex items-center justify-center text-xs text-slate-400">
            Loading...
          </div>
        ) : rows.length === 0 ? (
          <div className="h-[240px] flex flex-col items-center justify-center text-xs text-slate-400">
            <BarChart3 size={20} className="text-slate-300 mb-2" />
            No purchases recorded yet.
          </div>
        ) : (
          <ul className="space-y-2.5">
            {rows.map((row, i) => (
              <BarRow key={row.family} row={row} isTop={i === 0 && !row.isOther} />
            ))}
          </ul>
        )}
      </div>
    </Card>
  );
}

function BarRow({ row, isTop }) {
  // Use the largest visible bar's pct as the scale anchor so smaller bars
  // remain comparable. We pass anchor through `row.scaleMax`.
  const fillPct = Math.max(2, Math.min(100, (row.pct / row.scaleMax) * 100));
  return (
    <li>
      <div className="flex items-baseline justify-between gap-2 mb-1">
        <div className="flex items-center gap-1.5 min-w-0 flex-1">
          <span
            className="inline-block w-2.5 h-2.5 rounded-sm flex-shrink-0"
            style={{ backgroundColor: row.color }}
          />
          <span
            className={`text-xs truncate ${isTop ? 'font-bold text-mck-navy' : 'font-medium text-mck-navy'}`}
            title={row.family}
          >
            {row.family}
          </span>
          {isTop ? (
            <span className="text-[9px] uppercase tracking-wider font-bold text-mck-orange bg-mck-orange/10 border border-mck-orange/30 px-1.5 py-0.5 rounded flex-shrink-0">
              Top
            </span>
          ) : null}
        </div>
        <div className="flex items-baseline gap-2 flex-shrink-0 tabular-nums">
          <span className="text-[11px] text-slate-500">{formatCurrency(row.revenue)}</span>
          <span className="text-xs font-semibold text-mck-navy w-12 text-right">
            {row.pctLabel}
          </span>
        </div>
      </div>
      <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
        <div
          className="h-full rounded-full transition-all"
          style={{ width: `${fillPct}%`, backgroundColor: row.color }}
        />
      </div>
    </li>
  );
}

function buildRows(topFamilies) {
  if (!Array.isArray(topFamilies) || topFamilies.length === 0) {
    return { rows: [], totalRevenue: 0 };
  }

  const normalized = topFamilies
    .map((f, i) => ({
      family: f.family || 'Unknown',
      revenue: parseFloat(f.revenue) || 0,
      pct:
        typeof f.pct_of_total_revenue === 'number'
          ? f.pct_of_total_revenue
          : parseFloat(f.pct_of_total_revenue) || 0,
      color: PALETTE[i % PALETTE.length],
      isOther: false
    }))
    .filter((f) => f.revenue > 0)
    .sort((a, b) => b.revenue - a.revenue);

  if (normalized.length === 0) return { rows: [], totalRevenue: 0 };

  // Total revenue is the customer's full-period total. We back-derive it
  // from any row's revenue/pct pair (more reliable than the sum of returned
  // rows, which by definition omits the long tail).
  let derivedTotal = 0;
  for (let i = 0; i < normalized.length; i = i + 1) {
    const r = normalized[i];
    if (r.pct > 0) {
      derivedTotal = r.revenue / (r.pct / 100);
      break;
    }
  }
  // Fallback: if no pct came back, use the sum of visible rows
  if (derivedTotal === 0) {
    for (let i = 0; i < normalized.length; i = i + 1) {
      derivedTotal = derivedTotal + normalized[i].revenue;
    }
  }

  // Compute "Other" bucket = everything beyond the top families
  const visiblePctSum = normalized.reduce((s, r) => s + r.pct, 0);
  const visibleRevSum = normalized.reduce((s, r) => s + r.revenue, 0);
  const otherPct = Math.max(0, 100 - visiblePctSum);
  const otherRev = Math.max(0, derivedTotal - visibleRevSum);

  const rows = normalized.map((r) => ({
    ...r,
    pctLabel: `${r.pct.toFixed(1)}%`
  }));

  if (otherPct > 0.5 && otherRev > 0) {
    rows.push({
      family: 'Other categories',
      revenue: otherRev,
      pct: otherPct,
      pctLabel: `${otherPct.toFixed(1)}%`,
      color: OTHER_COLOR,
      isOther: true
    });
  }

  // Anchor the scale to the top bar so all bars share the same max for
  // accurate visual comparison.
  const scaleMax = rows[0].pct;
  for (let i = 0; i < rows.length; i = i + 1) {
    rows[i].scaleMax = scaleMax;
  }

  return { rows, totalRevenue: derivedTotal };
}
