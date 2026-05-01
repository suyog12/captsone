import { Users, Building2, ShoppingBag, DollarSign } from 'lucide-react';
import AppShell from '../components/shell/AppShell.jsx';
import Card, { CardHeader } from '../components/ui/Card.jsx';
import StatCard from '../components/ui/StatCard.jsx';
import SignalBadge from '../components/ui/SignalBadge.jsx';
import PurposeBadge from '../components/ui/PurposeBadge.jsx';
import LifecycleBadge from '../components/ui/LifecycleBadge.jsx';
import ConfidenceBar from '../components/ui/ConfidenceBar.jsx';
import { listSignals } from '../lib/signals.js';
import { listPurposes } from '../lib/purposes.js';
import { listLifecycles } from '../lib/lifecycle.js';

// Design system preview (temporary)

export default function DesignSystemPreview() {
  return (
    <AppShell title="Design system preview" subtitle="Sanity-check for chunk 2">
      <div className="space-y-6">
        {/* Stat cards */}
        <div>
          <h2 className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">
            Stat cards
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <StatCard label="Total customers" value="207,317" icon={Building2} accent="mck-blue" />
            <StatCard label="Total sellers" value="42" icon={Users} accent="mck-orange" />
            <StatCard
              label="MB penetration"
              value="57.3%"
              hint="across all recommendations"
              icon={ShoppingBag}
              accent="mck-navy"
            />
            <StatCard label="Family hit rate" value="91.8%" icon={DollarSign} accent="green" />
          </div>
        </div>

        {/* Signal badges */}
        <Card>
          <CardHeader title="Signal badges" subtitle="All 8 signal types from the recommendation engine" />
          <div className="flex flex-wrap gap-2">
            {listSignals().map((s) => (
              <SignalBadge key={s.key} signal={s.key} size="md" />
            ))}
          </div>
        </Card>

        {/* Purpose badges */}
        <Card>
          <CardHeader title="rec_purpose tags" subtitle="Why a recommendation is being shown" />
          <div className="flex flex-wrap gap-2">
            {listPurposes().map((p) => (
              <PurposeBadge key={p.key} purpose={p.key} size="md" />
            ))}
          </div>
        </Card>

        {/* Lifecycle badges */}
        <Card>
          <CardHeader title="Lifecycle status" subtitle="Customer health states" />
          <div className="flex flex-wrap gap-2">
            {listLifecycles().map((l) => (
              <LifecycleBadge key={l.key} status={l.key} size="md" />
            ))}
          </div>
        </Card>

        {/* Confidence bars */}
        <Card>
          <CardHeader title="Confidence bars" subtitle="High / medium / low tiers" />
          <div className="space-y-3 max-w-md">
            <ConfidenceBar tier="high" />
            <ConfidenceBar tier="medium" />
            <ConfidenceBar tier="low" />
          </div>
        </Card>
      </div>
    </AppShell>
  );
}
