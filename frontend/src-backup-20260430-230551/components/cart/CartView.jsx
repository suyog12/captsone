import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { ShoppingCart } from 'lucide-react';
import { getCart, updateCartQuantity, deleteCartItem } from '../../api.js';
import Card from '../ui/Card.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import CartItemRow from './CartItemRow.jsx';
import { formatCurrency, formatNumber } from '../../lib/format.js';

// Cart view

// Fetches and renders the cart for a given customer. Set readOnly=1 to
// hide the qty stepper and remove button (admin viewing mode).

export default function CartView({ custId, readOnly = 0 }) {
  const queryClient = useQueryClient();
  const [busyItemId, setBusyItemId] = useState(null);

  const { data, isLoading, isError, refetch } = useQuery({
    queryKey: ['cart', custId],
    queryFn: () => getCart(custId),
    enabled: Boolean(custId)
  });

  const updateMutation = useMutation({
    mutationFn: ({ cartItemId, quantity }) => updateCartQuantity(cartItemId, quantity),
    onMutate: ({ cartItemId }) => setBusyItemId(cartItemId),
    onSettled: () => {
      setBusyItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
    }
  });

  const deleteMutation = useMutation({
    mutationFn: (cartItemId) => deleteCartItem(cartItemId),
    onMutate: (cartItemId) => setBusyItemId(cartItemId),
    onSettled: () => {
      setBusyItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
    }
  });

  if (isLoading) return <FullPanelSpinner label="Loading cart" />;
  if (isError) return <EmptyState title="Could not load cart" description="Please try again." />;

  const items = (data && data.items) || [];
  const totalQty = (data && data.total_quantity) || 0;
  const subtotal = (data && data.estimated_total) || 0;

  if (items.length === 0) {
    return (
      <Card>
        <EmptyState
          icon={ShoppingCart}
          title="Cart is empty"
          description={readOnly === 1 ? 'This customer has no items in their cart right now.' : 'Add a recommendation to the cart, or browse products to start an order.'}
        />
      </Card>
    );
  }

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 border-b border-slate-100 flex items-start justify-between flex-wrap gap-2">
        <div>
          <h3 className="text-sm font-semibold text-mck-navy">Active cart</h3>
          <p className="text-xs text-slate-500 mt-0.5">
            {formatNumber(items.length)} {items.length === 1 ? 'line' : 'lines'} &middot; {formatNumber(totalQty)} {totalQty === 1 ? 'unit' : 'units'}
          </p>
        </div>
        <div className="text-right">
          <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">Estimated total</div>
          <div className="text-xl font-bold text-mck-navy">{formatCurrency(subtotal)}</div>
        </div>
      </div>

      <div className="divide-y divide-slate-100">
        {items.map((item) => (
          <CartItemRow
            key={item.cart_item_id}
            item={item}
            readOnly={readOnly}
            busy={busyItemId === item.cart_item_id ? 1 : 0}
            onUpdateQuantity={(cartItemId, qty) => updateMutation.mutate({ cartItemId, quantity: qty })}
            onRemove={(cartItemId) => deleteMutation.mutate(cartItemId)}
          />
        ))}
      </div>
    </Card>
  );
}
