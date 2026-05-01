import { useQuery } from '@tanstack/react-query';
import { Activity, TrendingUp, Check, X, AlertCircle } from 'lucide-react';
import Card, { CardHeader } from '../ui/Card.jsx';
import { getEngineEffectiveness } from '../../api.js';
import { formatCurrency, formatNumber } from '../../lib/format.js';

// Engine effectiveness panel

// Admin-only panel showing the recommendation engine's funnel:
//   adds (cart) -> sold | not_sold | rejected
// Per-signal breakdown plus a top-level summary.

const REASON_LABELS = {
  not_relevant:         'Not relevant to customer',
  already_have:         'Customer already has it',
  out_of_stock:         'Out of stock',
  price_too_high:       'Price too high',
  wrong_size_or_spec:   'Wrong size/spec',
  different_brand:      'Customer prefers different brand',
  bad_timing:           'Bad timing',
  wrong_recommendation: 'Engine error',
  other:                'Other'
};

export default function EngineEffectivenessPanel() {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['admin', 'engine-effectiveness'],
    queryFn: getEngineEffectiveness
  });

  if (isLoading) {
    return (
      <Card padding="lg">
        <div className="h-32 bg-slate-50 rounded animate-pulse" />
      </Card>
    );
  }

  if (isError || !data) {
    return (
      <Card padding="lg">
        <div className="text-sm text-slate-500">Could not load engine effectiveness data.</div>
      </Card>
    );
  }

  const totals = data.totals || {};
  const bySignal = data.by_signal || [];
  const byReason = data.by_reason || [];

  const hasData = (totals.cart_adds || 0) + (totals.rejected || 0) > 0;

  return (
    <Card padding="none">
      <CardHeader
        title="Engine effectiveness"
        subtitle="Per-signal funnel: cart adds -> sold + rejected. The engine's performance is measured by acceptance and conversion rates per signal."
      />

      {!hasData ? (
        <div className="px-5 pb-5">
          <div className="text-sm text-slate-500 italic">
            No recommendation engagement yet. Once sellers add or reject recommendations, the funnel will populate here.
          </div>
        </div>
      ) : (
        <div className="px-5 pb-5 space-y-5">
          {/* Top-line totals */}
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <SummaryStat
              icon={TrendingUp}
              accent="mck-blue"
              label="Cart adds (engine)"
              value={formatNumber(totals.cart_adds)}
              hint={`${formatCurrency(totals.revenue)} attributed revenue`}
            />
            <SummaryStat
              icon={Check}
              accent="green"
              label="Sold"
              value={formatNumber(totals.sold)}
              hint={`${(totals.conversion_rate_pct || 0).toFixed(1)}% conversion`}
            />
            <SummaryStat
              icon={X}
              accent="red"
              label="Rejected"
              value={formatNumber(totals.rejected)}
              hint={`${(totals.rejection_rate_pct || 0).toFixed(1)}% rejection rate`}
            />
            <SummaryStat
              icon={Activity}
              accent="mck-orange"
              label="Engagement total"
              value={formatNumber(totals.engaged)}
              hint={`${(totals.acceptance_rate_pct || 0).toFixed(1)}% acceptance rate`}
            />
          </div>

          {/* Per-signal breakdown */}
          <div>
            <h4 className="text-xs font-semibold text-slate-600 uppercase tracking-wider mb-2">
              By signal
            </h4>
            {bySignal.length === 0 ? (
              <div className="text-xs text-slate-400 italic">No per-signal data yet.</div>
            ) : (
              <div className="overflow-x-auto -mx-1 px-1">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="text-[10px] uppercase tracking-wider text-slate-500 border-b border-slate-200">
                      <th className="text-left py-2 font-semibold">Signal</th>
                      <th className="text-right py-2 font-semibold">Cart adds</th>
                      <th className="text-right py-2 font-semibold">Sold</th>
                      <th className="text-right py-2 font-semibold">Rejected</th>
                      <th className="text-right py-2 font-semibold">Conversion</th>
                      <th className="text-right py-2 font-semibold">Rejection</th>
                      <th className="text-right py-2 font-semibold">Revenue</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    {bySignal.map((row) => (
                      <SignalRow key={row.signal.code} row={row} />
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {/* Rejection reasons breakdown */}
          {byReason.length > 0 ? (
            <div>
              <h4 className="text-xs font-semibold text-slate-600 uppercase tracking-wider mb-2">
                Top rejection reasons
              </h4>
              <ul className="space-y-1.5">
                {byReason.slice(0, 6).map((r) => (
                  <li key={r.code} className="flex items-center justify-between text-xs">
                    <span className="text-mck-navy font-medium">
                      {REASON_LABELS[r.code] || r.code}
                    </span>
                    <span className="font-semibold tabular-nums">
                      {formatNumber(r.count)}
                    </span>
                  </li>
                ))}
              </ul>
            </div>
          ) : null}
        </div>
      )}
    </Card>
  );
}

function SignalRow({ row }) {
  // Color the conversion cell green/yellow/red based on rate
  const conv = row.conversion_rate_pct || 0;
  let convClass = 'text-slate-700';
  if (conv >= 60) convClass = 'text-green-700 font-semibold';
  else if (conv >= 30) convClass = 'text-amber-700';
  else if (row.cart_adds > 0) convClass = 'text-red-700';

  // Color rejection: red high, slate low
  const rej = row.rejection_rate_pct || 0;
  let rejClass = 'text-slate-500';
  if (rej >= 30) rejClass = 'text-red-700 font-semibold';
  else if (rej >= 10) rejClass = 'text-amber-700';

  return (
    <tr className="hover:bg-slate-50/50">
      <td className="py-2 text-mck-navy font-medium">{row.signal.display_name}</td>
      <td className="text-right tabular-nums">{formatNumber(row.cart_adds)}</td>
      <td className="text-right tabular-nums text-green-700">{formatNumber(row.sold)}</td>
      <td className="text-right tabular-nums text-red-600">{formatNumber(row.rejected)}</td>
      <td className={`text-right tabular-nums ${convClass}`}>
        {row.cart_adds > 0 ? `${conv.toFixed(1)}%` : '-'}
      </td>
      <td className={`text-right tabular-nums ${rejClass}`}>
        {row.engaged > 0 ? `${rej.toFixed(1)}%` : '-'}
      </td>
      <td className="text-right tabular-nums text-mck-navy">
        {formatCurrency(row.revenue)}
      </td>
    </tr>
  );
}

function SummaryStat({ icon: Icon, accent, label, value, hint }) {
  const accentMap = {
    'mck-blue': 'border-l-mck-blue text-mck-blue',
    'mck-orange': 'border-l-mck-orange text-mck-orange',
    green: 'border-l-green-500 text-green-600',
    red: 'border-l-red-500 text-red-600'
  };
  const cls = accentMap[accent] || accentMap['mck-blue'];
  return (
    <div className={`bg-white rounded-md border border-slate-200 border-l-4 ${cls.split(' ')[0]} px-4 py-3`}>
      <div className="flex items-start justify-between">
        <div className="flex-1 min-w-0">
          <div className="text-[10px] font-semibold text-slate-500 uppercase tracking-wider">{label}</div>
          <div className="text-xl font-bold text-mck-navy mt-0.5 leading-tight">{value}</div>
          {hint ? <div className="text-[11px] text-slate-500 mt-0.5">{hint}</div> : null}
        </div>
        <Icon size={20} className={`flex-shrink-0 ml-2 ${cls.split(' ')[1]}`} />
      </div>
    </div>
  );
}
