import { useQuery } from '@tanstack/react-query';
import { Activity, AlertCircle } from 'lucide-react';
import Card, { CardHeader } from '../ui/Card.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import { getChurnFunnel } from '../../api.js';
import { formatNumber } from '../../lib/format.js';

// Customer lifecycle funnel - horizontal stacked bar
// showing the distribution of customers across the four lifecycle
// stages (cold start, stable, declining, churned). Percentages sum to
// 100%; the legend below the bar gives counts in absolute terms.

const COLOR_MAP = {
  blue:   { bar: 'bg-mck-blue',    text: 'text-mck-blue',    dot: 'bg-mck-blue' },
  green:  { bar: 'bg-emerald-500', text: 'text-emerald-700', dot: 'bg-emerald-500' },
  orange: { bar: 'bg-mck-orange',  text: 'text-orange-700',  dot: 'bg-mck-orange' },
  red:    { bar: 'bg-red-500',     text: 'text-red-700',     dot: 'bg-red-500' },
  gray:   { bar: 'bg-slate-400',   text: 'text-slate-600',   dot: 'bg-slate-400' }
};

export default function ChurnFunnelCard() {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['admin', 'churn-funnel'],
    queryFn: getChurnFunnel
  });

  if (isLoading) {
    return (
      <Card>
        <CardHeader
          title="Customer lifecycle"
          subtitle="Distribution across cold start, stable, declining, and churned"
        />
        <div className="h-32 bg-slate-50 rounded animate-pulse" />
      </Card>
    );
  }

  if (isError || !data) {
    return (
      <Card>
        <CardHeader title="Customer lifecycle" />
        <EmptyState
          icon={AlertCircle}
          title="Could not load lifecycle distribution"
          description="Please try again in a moment."
        />
      </Card>
    );
  }

  const total = data.total || 0;
  const stages = data.stages || [];

  if (total === 0) {
    return (
      <Card>
        <CardHeader title="Customer lifecycle" />
        <EmptyState title="No customer data" description="Lifecycle status not available yet." />
      </Card>
    );
  }

  // Computed: at-risk count = declining + churned
  const atRisk = stages
    .filter((s) => s.status === 'declining_warm' || s.status === 'churned_warm')
    .reduce((acc, s) => acc + s.count, 0);
  const atRiskPct = total > 0 ? (atRisk / total) * 100 : 0;

  return (
    <Card>
      <CardHeader
        title="Customer lifecycle"
        subtitle={`${formatNumber(total)} customers across four lifecycle stages`}
        action={
          <div className="text-right">
            <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">At risk</div>
            <div className="text-base font-bold text-mck-navy">
              {formatNumber(atRisk)}{' '}
              <span className="text-xs font-medium text-slate-500">
                ({atRiskPct.toFixed(1)}%)
              </span>
            </div>
          </div>
        }
      />

      {/* Stacked horizontal bar */}
      <div className="space-y-4 pt-2">
        <div className="flex w-full h-10 rounded-md overflow-hidden border border-slate-200 bg-slate-50">
          {stages.map((stage) => {
            const colors = COLOR_MAP[stage.color] || COLOR_MAP.blue;
            // Render zero-width segments invisibly so they don't break the bar
            const widthPct = stage.pct;
            if (widthPct < 0.01) return null;
            // Show inline label only if segment is wide enough (~10%)
            const showInlineLabel = widthPct >= 8;
            return (
              <div
                key={stage.status}
                className={`${colors.bar} flex items-center justify-center text-white text-xs font-semibold transition-all`}
                style={{ width: `${widthPct}%` }}
                title={`${stage.label}: ${formatNumber(stage.count)} (${stage.pct.toFixed(1)}%)`}
              >
                {showInlineLabel ? (
                  <span className="px-2 truncate">
                    {stage.label} {stage.pct.toFixed(0)}%
                  </span>
                ) : null}
              </div>
            );
          })}
        </div>

        {/* Legend below the bar - one row per stage */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
          {stages.map((stage) => {
            const colors = COLOR_MAP[stage.color] || COLOR_MAP.blue;
            return (
              <div key={stage.status} className="flex items-center gap-2">
                <span className={`w-2.5 h-2.5 rounded-full flex-shrink-0 ${colors.dot}`} />
                <div className="min-w-0">
                  <div className={`text-xs font-semibold ${colors.text} truncate`}>
                    {stage.label}
                  </div>
                  <div className="text-xs text-slate-500">
                    {formatNumber(stage.count)} <span className="text-slate-400">({stage.pct.toFixed(1)}%)</span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </Card>
  );
}