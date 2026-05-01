import { useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  ArrowLeft,
  Sparkles,
  ShoppingCart,
  ClipboardList,
  BarChart3,
  Building2,
  Mail,
  Phone,
  MapPin,
  User,
  Briefcase,
  Lock,
  UserPlus,
  Loader2,
  Check
,
  Package} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import LifecycleBadge from '../../components/ui/LifecycleBadge.jsx';
import Tabs from '../../components/ui/Tabs.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import RecommendationList from '../../components/recs/RecommendationList.jsx';
import CartView from '../../components/cart/CartView.jsx';
import CartHelperPanel from '../../components/cart/CartHelperPanel.jsx';
import OrderHistoryView from '../../components/history/OrderHistoryView.jsx';
import PerformancePanel from '../../components/perf/PerformancePanel.jsx';
import CatalogBrowse from '../../components/catalog/CatalogBrowse.jsx';
import { getCustomer, getCustomerStats, claimCustomer } from '../../api.js';
import { useAuth } from '../../auth.jsx';
import { formatDate } from '../../lib/format.js';

// Seller customer profile

const TABS = [
  { value: 'recs', label: 'Recommendations', icon: Sparkles },
  { value: 'cart', label: 'Cart & Cart-helper', icon: ShoppingCart },
  { value: 'history', label: 'Order history', icon: ClipboardList },
  { value: 'performance', label: 'Performance', icon: BarChart3 },
  { value: 'catalog', label: 'Browse catalog', icon: Package }
];

export default function SellerCustomerProfile() {
  const { custId } = useParams();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const { user } = useAuth();
  const [tab, setTab] = useState('recs');

  const numericId = Number(custId);

  const {
    data: customer,
    isLoading,
    isError,
    error
  } = useQuery({
    queryKey: ['customer', numericId],
    queryFn: () => getCustomer(numericId),
    enabled: Number.isFinite(numericId)
  });

  const claimMutation = useMutation({
    mutationFn: () => claimCustomer(numericId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['customer', numericId] });
      queryClient.invalidateQueries({ queryKey: ['seller', 'me', 'customers'] });
      queryClient.invalidateQueries({ queryKey: ['seller', 'all-customers'] });
    }
  });

  if (isLoading) {
    return (
      <AppShell title="Loading customer">
        <FullPanelSpinner label="Loading customer profile" />
      </AppShell>
    );
  }

  if (isError || !customer) {
    const detail = error && error.response && error.response.data && error.response.data.detail;
    const msg = (typeof detail === 'string' && detail) || 'This customer could not be loaded.';
    return (
      <AppShell title="Customer not found">
        <EmptyState
          icon={Building2}
          title="Customer not found"
          description={msg}
          action={
            <button
              type="button"
              onClick={() => navigate('/seller')}
              className="px-3 py-1.5 text-sm bg-mck-blue text-white rounded hover:bg-mck-blue-dark"
            >
              Back to my customers
            </button>
          }
        />
      </AppShell>
    );
  }

  // Determine relationship: my customer, unassigned, or another seller's
  const myUserId = user && user.user_id;
  const assignedToMe =
    customer.is_assigned_to_me === true ||
    customer.assigned_seller_id === myUserId;
  const unassigned =
    customer.assigned_seller_id === null || customer.assigned_seller_id === undefined;
  const otherSeller = !assignedToMe && !unassigned;

  // Read-only when viewing another seller's customer
  const readOnly = otherSeller || unassigned;

  return (
    <AppShell
      title={customer.customer_name || `Customer ${customer.cust_id}`}
      subtitle={`#${customer.cust_id}`}
      actions={
        <button
          type="button"
          onClick={() => navigate('/seller')}
          className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-600 hover:text-mck-navy px-2 py-1 rounded hover:bg-slate-100"
        >
          <ArrowLeft size={14} />
          Back
        </button>
      }
    >
      <div className="space-y-6">
        {/* Relationship banner */}
        {unassigned ? (
          <ClaimBanner
            onClaim={() => claimMutation.mutate()}
            pending={claimMutation.isPending}
            success={claimMutation.isSuccess}
            error={claimMutation.isError}
          />
        ) : null}

        {otherSeller ? <OtherSellerBanner sellerId={customer.assigned_seller_id} /> : null}

        <ProfileHeader customer={customer} />

        <div>
          <Tabs tabs={TABS} value={tab} onChange={setTab} />
          <div className="pt-5">
            {tab === 'recs' ? (
              <RecommendationList custId={numericId} allowAddToCart={assignedToMe ? 1 : 0} />
            ) : null}

            {tab === 'cart' ? (
              readOnly ? (
                <ReadOnlyTabNotice
                  reason={
                    unassigned
                      ? 'Claim this customer first to manage their cart.'
                      : 'This customer is assigned to another seller. Cart actions are reserved for the assigned seller.'
                  }
                />
              ) : (
                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                  <div className="lg:col-span-2">
                    <CartView custId={numericId} readOnly={0} />
                  </div>
                  <div>
                    <CartHelperPanel custId={numericId} />
                  </div>
                </div>
              )
            ) : null}

            {tab === 'history' ? <OrderHistoryView custId={numericId} /> : null}

            {tab === 'performance' ? (
              <PerformancePanel
                queryKey={['customer', numericId, 'stats']}
                fetcher={(params) => getCustomerStats(numericId, params)}
                title="Customer performance"
                subtitle="Revenue and product trends for this customer"
              />
            ) : null}
            {tab === 'catalog' ? (
              readOnly ? (
                <ReadOnlyTabNotice
                  reason={
                    unassigned
                      ? 'Claim this customer first to add catalog items to their cart.'
                      : 'This customer is assigned to another seller. Read-only browse only.'
                  }
                />
              ) : (
                <CatalogBrowse
                  custId={numericId}
                  allowAddToCart={1}
                  ctaSource="manual"
                />
              )
            ) : null}
          </div>
        </div>
      </div>
    </AppShell>
  );
}

function ClaimBanner({ onClaim, pending, success, error }) {
  if (success) {
    return (
      <div className="bg-green-50 border border-green-200 rounded-lg px-4 py-3 flex items-center gap-3">
        <Check size={16} className="text-green-600 flex-shrink-0" />
        <div className="flex-1 text-sm text-green-800">
          <span className="font-semibold">Claimed.</span> This customer is now in your portfolio.
        </div>
      </div>
    );
  }

  return (
    <div className="bg-mck-sky/40 border border-mck-blue/30 rounded-lg px-4 py-3 flex items-center gap-3 flex-wrap">
      <UserPlus size={16} className="text-mck-blue flex-shrink-0" />
      <div className="flex-1 min-w-[12rem]">
        <div className="text-sm font-semibold text-mck-navy">This customer is unassigned</div>
        <div className="text-xs text-slate-600 mt-0.5">
          Claim them to add to your portfolio and unlock cart actions.
        </div>
        {error ? (
          <div className="text-xs text-red-600 mt-1">Could not claim. Please try again.</div>
        ) : null}
      </div>
      <button
        type="button"
        onClick={onClaim}
        disabled={pending}
        className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium bg-mck-blue text-white rounded hover:bg-mck-blue-dark disabled:opacity-60 disabled:cursor-not-allowed"
      >
        {pending ? <Loader2 size={12} className="animate-spin" /> : <UserPlus size={12} />}
        {pending ? 'Claiming...' : 'Claim customer'}
      </button>
    </div>
  );
}

function OtherSellerBanner({ sellerId }) {
  return (
    <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3 flex items-center gap-3">
      <Lock size={14} className="text-slate-500 flex-shrink-0" />
      <div className="text-sm text-slate-700">
        <span className="font-semibold">Read-only view.</span> This customer is assigned to seller{' '}
        <span className="font-mono">#{sellerId}</span>. You can view their profile and recommendations
        but cart actions are reserved for the assigned seller.
      </div>
    </div>
  );
}

function ReadOnlyTabNotice({ reason }) {
  return (
    <Card>
      <div className="py-8 text-center">
        <Lock size={20} className="text-slate-400 mx-auto mb-2" />
        <div className="text-base font-semibold text-mck-navy">Read-only</div>
        <div className="text-sm text-slate-500 mt-1 max-w-md mx-auto">{reason}</div>
      </div>
    </Card>
  );
}

function ProfileHeader({ customer }) {
  return (
    <Card padding="lg">
      <div className="flex items-start gap-5 flex-wrap">
        <div className="flex-shrink-0 w-16 h-16 rounded-lg bg-mck-sky text-mck-blue font-bold text-xl flex items-center justify-center">
          {(customer.customer_name || `${customer.cust_id}`).slice(0, 2).toUpperCase()}
        </div>

        <div className="flex-1 min-w-[18rem]">
          <div className="flex items-center gap-2 flex-wrap">
            <h2 className="text-xl font-semibold text-mck-navy">
              {customer.customer_name || `Customer ${customer.cust_id}`}
            </h2>
            <LifecycleBadge status={customer.status} size="md" />
          </div>

          <div className="mt-2 grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-2 text-xs">
            <Field icon={Building2} label="Cust ID" value={`#${customer.cust_id}`} />
            <Field icon={Briefcase} label="Segment" value={customer.segment || '-'} />
            <Field icon={MapPin} label="Market" value={customer.market_code || '-'} />
            <Field icon={User} label="Specialty" value={customer.specialty_code || '-'} />
            <Field icon={Building2} label="Archetype" value={prettyArchetype(customer.archetype)} />
            <Field icon={ClipboardList} label="Supplier profile" value={customer.supplier_profile || '-'} />
            <Field icon={Mail} label="Assigned at" value={formatDate(customer.assigned_at)} />
            <Field icon={Phone} label="Created" value={formatDate(customer.created_at)} />
          </div>
        </div>
      </div>
    </Card>
  );
}

function prettyArchetype(s) {
  if (!s) return '-';
  return s.charAt(0).toUpperCase() + s.slice(1).replace(/_/g, ' ');
}

function Field({ icon: Icon, label, value }) {
  return (
    <div className="flex items-start gap-2 min-w-0">
      <Icon size={13} className="text-slate-400 flex-shrink-0 mt-0.5" />
      <div className="min-w-0">
        <div className="text-[10px] uppercase tracking-wider text-slate-500 font-semibold">{label}</div>
        <div className="text-xs text-mck-navy font-medium truncate">{value}</div>
      </div>
    </div>
  );
}
