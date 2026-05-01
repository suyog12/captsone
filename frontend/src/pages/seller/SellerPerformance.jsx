import AppShell from '../../components/shell/AppShell.jsx';
import PerformancePanel from '../../components/perf/PerformancePanel.jsx';
import { getMySellerStats } from '../../api.js';
import { useAuth } from '../../auth.jsx';

// Seller performance

export default function SellerPerformance() {
  const { user } = useAuth();
  return (
    <AppShell title="My performance" subtitle="Your portfolio's revenue, top products, and trend">
      <PerformancePanel
        queryKey={['seller', 'me', 'stats']}
        fetcher={getMySellerStats}
        title="Performance"
        subtitle={user ? `For ${user.full_name || user.username}` : ''}
      />
    </AppShell>
  );
}
