import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { useState } from 'react';
import { ShoppingCart, CheckCheck, Loader2 } from 'lucide-react';
import {
  getCart,
  updateCartQuantity,
  deleteCartItem,
  updateCartStatus,
  checkoutCartItem
} from '../../api.js';
import Card from '../ui/Card.jsx';
import { FullPanelSpinner } from '../ui/Spinner.jsx';
import EmptyState from '../ui/EmptyState.jsx';
import CartItemRow from './CartItemRow.jsx';
import { formatCurrency, formatNumber } from '../../lib/format.js';

// Cart view

// Fetches and renders the cart for a given customer.
//
// Modes:
//   readOnly=1            -> hide all editing/closing controls (admin viewing)
//   readOnly=0, sellerMode=0 -> customer view: qty stepper + remove button
//   readOnly=0, sellerMode=1 -> seller view: qty stepper + remove + per-line
//                            Mark Sold (POST /cart/{id}/checkout) and
//                            Mark Not Sold (PATCH /cart/{id}/status), plus
//                            a Close All Sold bulk button below the list.

export default function CartView({ custId, readOnly = 0, sellerMode = 0 }) {
  const queryClient = useQueryClient();
  const [busyItemId, setBusyItemId] = useState(null);
  const [bulkBusy, setBulkBusy] = useState(0);
  const [bulkProgress, setBulkProgress] = useState({ done: 0, total: 0 });

  const { data, isLoading, isError } = useQuery({
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

  const checkoutMutation = useMutation({
    mutationFn: (cartItemId) => checkoutCartItem(cartItemId),
    onMutate: (cartItemId) => setBusyItemId(cartItemId),
    onSettled: () => {
      setBusyItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
      queryClient.invalidateQueries({ queryKey: ['cart-helper', custId] });
      queryClient.invalidateQueries({ queryKey: ['seller', 'me', 'stats'] });
    }
  });

  const statusMutation = useMutation({
    mutationFn: ({ cartItemId, status }) => updateCartStatus(cartItemId, status),
    onMutate: ({ cartItemId }) => setBusyItemId(cartItemId),
    onSettled: () => {
      setBusyItemId(null);
      queryClient.invalidateQueries({ queryKey: ['cart', custId] });
      queryClient.invalidateQueries({ queryKey: ['cart-helper', custId] });
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
          description={
            readOnly === 1
              ? 'This customer has no items in their cart right now.'
              : sellerMode === 1
              ? 'No active items to close. Add items from recommendations or browse the catalog to start an order.'
              : 'Add a recommendation to the cart, or browse products to start an order.'
          }
        />
      </Card>
    );
  }

  // Bulk close: fire one checkout per in_cart line sequentially.
  async function handleCloseAllSold() {
    const ids = items.map((it) => it.cart_item_id);
    if (ids.length === 0) return;
    setBulkBusy(1);
    setBulkProgress({ done: 0, total: ids.length });
    for (let i = 0; i < ids.length; i = i + 1) {
      try {
        await checkoutMutation.mutateAsync(ids[i]);
      } catch (err) {
        // Continue on error - we surface the failure via cart refresh
      }
      setBulkProgress({ done: i + 1, total: ids.length });
    }
    setBulkBusy(0);
    setBulkProgress({ done: 0, total: 0 });
  }

  return (
    <Card padding="none">
      <div className="px-5 pt-5 pb-3 border-b border-slate-100 flex items-start justify-between flex-wrap gap-2">
        <div>
          <h3 className="text-sm font-semibold text-mck-navy">Active cart</h3>
          <p className="text-xs text-slate-500 mt-0.5">
            {formatNumber(items.length)} {items.length === 1 ? 'line' : 'lines'} &middot;{' '}
            {formatNumber(totalQty)} {totalQty === 1 ? 'unit' : 'units'}
          </p>
        </div>
        <div className="text-right">
          <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">
            Estimated total
          </div>
          <div className="text-xl font-bold text-mck-navy">{formatCurrency(subtotal)}</div>
        </div>
      </div>

      <div className="divide-y divide-slate-100">
        {items.map((item) => (
          <CartItemRow
            key={item.cart_item_id}
            item={item}
            readOnly={readOnly}
            sellerMode={sellerMode}
            busy={busyItemId === item.cart_item_id ? 1 : 0}
            onUpdateQuantity={(cartItemId, qty) =>
              updateMutation.mutate({ cartItemId, quantity: qty })
            }
            onRemove={(cartItemId) => deleteMutation.mutate(cartItemId)}
            onMarkSold={(cartItemId) => checkoutMutation.mutate(cartItemId)}
            onMarkNotSold={(cartItemId) =>
              statusMutation.mutate({ cartItemId, status: 'not_sold' })
            }
          />
        ))}
      </div>

      {/* Bulk Close All Sold (seller-only) */}
      {sellerMode === 1 && readOnly === 0 ? (
        <div className="px-5 py-4 border-t border-slate-100 bg-slate-50 flex items-center justify-between gap-3 flex-wrap">
          <div className="text-xs text-slate-500">
            {bulkBusy === 1
              ? `Closing ${bulkProgress.done} of ${bulkProgress.total}...`
              : `Close all ${items.length} active line${items.length === 1 ? '' : 's'} as sold in one click.`}
          </div>
          <button
            type="button"
            onClick={handleCloseAllSold}
            disabled={bulkBusy === 1 || items.length === 0}
            className="inline-flex items-center gap-1.5 px-3 py-2 text-xs font-semibold rounded transition-colors bg-mck-orange text-white hover:bg-mck-orange-dark disabled:opacity-50 disabled:cursor-not-allowed"
          >
            {bulkBusy === 1 ? (
              <>
                <Loader2 size={12} className="animate-spin" />
                Closing...
              </>
            ) : (
              <>
                <CheckCheck size={12} />
                Close All Sold ({items.length})
              </>
            )}
          </button>
        </div>
      ) : null}
    </Card>
  );
}
