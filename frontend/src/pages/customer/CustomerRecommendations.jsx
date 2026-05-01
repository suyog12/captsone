import AppShell from '../../components/shell/AppShell.jsx';
import RecommendationList from '../../components/recs/RecommendationList.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import { Sparkles } from 'lucide-react';
import { useAuth } from '../../auth.jsx';

// Customer recommendations

export default function CustomerRecommendations() {
  const { user } = useAuth();
  const custId = user && user.cust_id;

  return (
    <AppShell title="Recommended for you" subtitle="Curated picks based on your specialty, segment, and purchase patterns">
      {custId ? (
        <RecommendationList custId={custId} allowAddToCart={1} />
      ) : (
        <EmptyState
          icon={Sparkles}
          title="No customer record linked"
          description="Your account is not linked to a customer profile. Contact your admin."
        />
      )}
    </AppShell>
  );
}
