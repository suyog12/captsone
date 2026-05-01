import AppShell from '../../components/shell/AppShell.jsx';
import OrderHistoryView from '../../components/history/OrderHistoryView.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import { ClipboardList } from 'lucide-react';
import { useAuth } from '../../auth.jsx';

// Customer orders

export default function CustomerOrders() {
  const { user } = useAuth();
  const custId = user && user.cust_id;

  if (!custId) {
    return (
      <AppShell title="Order history">
        <EmptyState icon={ClipboardList} title="No customer record linked" />
      </AppShell>
    );
  }

  return (
    <AppShell title="Order history" subtitle="Your complete purchase history">
      <OrderHistoryView custId={custId} />
    </AppShell>
  );
}
