import AppShell from '../../components/shell/AppShell.jsx';
import CartView from '../../components/cart/CartView.jsx';
import CartHelperPanel from '../../components/cart/CartHelperPanel.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import { ShoppingCart } from 'lucide-react';
import { useAuth } from '../../auth.jsx';

// Customer cart

export default function CustomerCart() {
  const { user } = useAuth();
  const custId = user && user.cust_id;

  if (!custId) {
    return (
      <AppShell title="My cart">
        <EmptyState
          icon={ShoppingCart}
          title="No customer record linked"
          description="Your account is not linked to a customer profile."
        />
      </AppShell>
    );
  }

  return (
    <AppShell title="My cart" subtitle="Active items and live suggestions">
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        <div className="lg:col-span-2">
          <CartView custId={custId} readOnly={0} />
        </div>
        <div>
          <CartHelperPanel custId={custId} />
        </div>
      </div>
    </AppShell>
  );
}
