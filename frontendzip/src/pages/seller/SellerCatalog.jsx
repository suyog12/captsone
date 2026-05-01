import { useParams, useNavigate } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { ArrowLeft, Lock } from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import CatalogBrowse from '../../components/catalog/CatalogBrowse.jsx';
import { getCustomer } from '../../api.js';
import { useAuth } from '../../auth.jsx';

// Seller catalog

export default function SellerCatalog() {
  const { custId } = useParams();
  const navigate = useNavigate();
  const { user } = useAuth();
  const numericId = Number(custId);

  const { data: customer, isLoading } = useQuery({
    queryKey: ['customer', numericId],
    queryFn: () => getCustomer(numericId),
    enabled: Number.isFinite(numericId)
  });

  if (isLoading) {
    return (
      <AppShell title="Loading">
        <FullPanelSpinner label="Loading customer" />
      </AppShell>
    );
  }

  if (!customer) {
    return (
      <AppShell title="Catalog">
        <EmptyState title="Customer not found" description="Go back and pick a customer first." />
      </AppShell>
    );
  }

  const myUserId = user && user.user_id;
  const assignedToMe =
    customer.is_assigned_to_me === true || customer.assigned_seller_id === myUserId;

  return (
    <AppShell
      title={`Catalog for ${customer.customer_name || `#${customer.cust_id}`}`}
      subtitle={`Browse the full product catalog and add items directly to ${customer.customer_name || 'this customer'}'s cart`}
      actions={
        <button
          type="button"
          onClick={() => navigate(`/seller/customers/${numericId}`)}
          className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-600 hover:text-mck-navy px-2 py-1 rounded hover:bg-slate-100"
        >
          <ArrowLeft size={14} />
          Back to profile
        </button>
      }
    >
      {!assignedToMe ? (
        <div className="mb-4">
          <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3 flex items-center gap-3">
            <Lock size={14} className="text-slate-500 flex-shrink-0" />
            <div className="text-sm text-slate-700">
              <span className="font-semibold">Read-only browse.</span> This customer is not assigned to you. Cart actions are reserved for the assigned seller.
            </div>
          </div>
        </div>
      ) : null}

      <CatalogBrowse
        custId={numericId}
        allowAddToCart={assignedToMe ? 1 : 0}
        ctaSource="manual"
      />
    </AppShell>
  );
}
