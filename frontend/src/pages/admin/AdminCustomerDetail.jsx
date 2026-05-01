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
  MapPin,
  User,
  Briefcase,
  UserCog,
  X,
  Check,
  Loader2,
  History
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import LifecycleBadge from '../../components/ui/LifecycleBadge.jsx';
import Tabs from '../../components/ui/Tabs.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import RecommendationList from '../../components/recs/RecommendationList.jsx';
import CartView from '../../components/cart/CartView.jsx';
import OrderHistoryView from '../../components/history/OrderHistoryView.jsx';
import PerformancePanel from '../../components/perf/PerformancePanel.jsx';
import {
  getCustomer,
  getCustomerStats,
  listUsers,
  changeAssignment,
  getAssignmentHistory
} from '../../api.js';
import { formatDate, relativeTime } from '../../lib/format.js';

// Admin customer detail

const TABS = [
  { value: 'recs', label: 'Recommendations', icon: Sparkles },
  { value: 'cart', label: 'Cart (read-only)', icon: ShoppingCart },
  { value: 'history', label: 'Order history', icon: ClipboardList },
  { value: 'performance', label: 'Performance', icon: BarChart3 },
  { value: 'assignment', label: 'Assignment history', icon: History }
];

export default function AdminCustomerDetail() {
  const { custId } = useParams();
  const navigate = useNavigate();
  const [tab, setTab] = useState('recs');
  const [reassignOpen, setReassignOpen] = useState(0);
  const numericId = Number(custId);

  const { data: customer, isLoading, isError, error } = useQuery({
    queryKey: ['admin', 'customer', numericId],
    queryFn: () => getCustomer(numericId),
    enabled: Number.isFinite(numericId)
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
              onClick={() => navigate('/admin/customers')}
              className="px-3 py-1.5 text-sm bg-mck-blue text-white rounded hover:bg-mck-blue-dark"
            >
              Back to customers
            </button>
          }
        />
      </AppShell>
    );
  }

  return (
    <AppShell
      title={customer.customer_name || `Customer ${customer.cust_id}`}
      subtitle={`#${customer.cust_id} (admin view)`}
      actions={
        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => setReassignOpen(1)}
            className="inline-flex items-center gap-1.5 text-xs font-semibold text-white bg-mck-blue hover:bg-mck-blue-dark px-3 py-1.5 rounded"
          >
            <UserCog size={13} />
            Reassign seller
          </button>
          <button
            type="button"
            onClick={() => navigate('/admin/customers')}
            className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-600 hover:text-mck-navy px-2 py-1 rounded hover:bg-slate-100"
          >
            <ArrowLeft size={14} />
            Back
          </button>
        </div>
      }
    >
      <div className="space-y-6">
        <ProfileHeader customer={customer} />

        <div>
          <Tabs tabs={TABS} value={tab} onChange={setTab} />
          <div className="pt-5">
            {tab === 'recs' ? <RecommendationList custId={numericId} allowAddToCart={0} /> : null}
            {tab === 'cart' ? <CartView custId={numericId} readOnly={1} /> : null}
            {tab === 'history' ? <OrderHistoryView custId={numericId} /> : null}
            {tab === 'performance' ? (
              <PerformancePanel
                queryKey={['admin', 'customer', numericId, 'stats']}
                fetcher={(params) => getCustomerStats(numericId, params)}
                title="Customer performance"
                subtitle="Revenue and product trends for this customer"
              />
            ) : null}
            {tab === 'assignment' ? <AssignmentHistory custId={numericId} /> : null}
          </div>
        </div>
      </div>

      {reassignOpen === 1 ? (
        <ReassignModal
          customer={customer}
          onClose={() => setReassignOpen(0)}
        />
      ) : null}
    </AppShell>
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
            <Field icon={UserCog} label="Assigned seller" value={customer.assigned_seller_id ? `#${customer.assigned_seller_id}` : 'Unassigned'} />
            <Field icon={Mail} label="Created" value={formatDate(customer.created_at)} />
          </div>
        </div>
      </div>
    </Card>
  );
}

function ReassignModal({ customer, onClose }) {
  const queryClient = useQueryClient();
  const [selectedSellerId, setSelectedSellerId] = useState(customer.assigned_seller_id || null);
  const [notes, setNotes] = useState('');

  const sellersQuery = useQuery({
    queryKey: ['admin', 'sellers-roster'],
    queryFn: () => listUsers({ role: 'seller', limit: 500, offset: 0 })
  });

  const sellers = (sellersQuery.data && sellersQuery.data.items) || [];
  const activeSellers = sellers.filter((s) => s.is_active === true || s.is_active === 1);

  const reassignMutation = useMutation({
    mutationFn: () => changeAssignment(customer.cust_id, selectedSellerId, notes || undefined),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'customer', customer.cust_id] });
      queryClient.invalidateQueries({ queryKey: ['admin', 'customers-filter'] });
      queryClient.invalidateQueries({ queryKey: ['admin', 'customer', customer.cust_id, 'assignment-history'] });
      onClose();
    }
  });

  function handleSubmit(e) {
    e.preventDefault();
    reassignMutation.mutate();
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-white rounded-lg shadow-xl max-w-md w-full">
        <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
          <div>
            <h3 className="text-base font-semibold text-mck-navy">Reassign seller</h3>
            <p className="text-xs text-slate-500 mt-0.5">For {customer.customer_name || `Customer ${customer.cust_id}`}</p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="text-slate-400 hover:text-slate-600 p-1 rounded hover:bg-slate-100"
          >
            <X size={16} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="px-5 py-4 space-y-4">
          <div>
            <label className="text-xs font-semibold text-slate-600 uppercase tracking-wider">Assign to seller</label>
            <select
              value={selectedSellerId === null ? '' : String(selectedSellerId)}
              onChange={(e) => setSelectedSellerId(e.target.value === '' ? null : Number(e.target.value))}
              className="mt-1 w-full text-sm border border-slate-200 rounded-md px-3 py-2 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue"
            >
              <option value="">Unassigned</option>
              {activeSellers.map((s) => (
                <option key={s.user_id} value={s.user_id}>
                  {s.full_name || s.username} (@{s.username})
                </option>
              ))}
            </select>
            <p className="text-[11px] text-slate-500 mt-1">
              {customer.assigned_seller_id
                ? `Currently assigned to seller #${customer.assigned_seller_id}.`
                : 'Currently unassigned.'}
            </p>
          </div>

          <div>
            <label className="text-xs font-semibold text-slate-600 uppercase tracking-wider">Notes (optional)</label>
            <textarea
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
              rows={3}
              placeholder="Reason for reassignment..."
              className="mt-1 w-full text-sm border border-slate-200 rounded-md px-3 py-2 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue placeholder:text-slate-400 resize-none"
            />
          </div>

          {reassignMutation.isError ? (
            <div className="bg-red-50 border border-red-200 rounded-md px-3 py-2 text-xs text-red-700">
              {(reassignMutation.error && reassignMutation.error.response && reassignMutation.error.response.data && reassignMutation.error.response.data.detail) || 'Reassignment failed.'}
            </div>
          ) : null}

          <div className="flex justify-end gap-2 pt-2 border-t border-slate-100">
            <button
              type="button"
              onClick={onClose}
              className="px-3 py-1.5 text-sm text-slate-600 hover:text-mck-navy rounded hover:bg-slate-100"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={reassignMutation.isPending}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-semibold text-white bg-mck-blue hover:bg-mck-blue-dark disabled:bg-slate-300 rounded"
            >
              {reassignMutation.isPending ? <Loader2 size={13} className="animate-spin" /> : <Check size={13} />}
              Save reassignment
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

function AssignmentHistory({ custId }) {
  const { data, isLoading, isError } = useQuery({
    queryKey: ['admin', 'customer', custId, 'assignment-history'],
    queryFn: () => getAssignmentHistory(custId, 100),
    enabled: Boolean(custId)
  });

  if (isLoading) return <FullPanelSpinner label="Loading assignment history" />;
  if (isError) return <EmptyState title="Could not load assignment history" />;

  const items = (data && data.items) || [];
  if (items.length === 0) {
    return (
      <Card>
        <EmptyState
          icon={History}
          title="No assignment changes recorded"
          description="There are no assignment changes on file for this customer."
        />
      </Card>
    );
  }

  return (
    <Card padding="none">
      <div className="px-5 py-3 border-b border-slate-100 text-xs text-slate-500">
        {items.length} {items.length === 1 ? 'change' : 'changes'}
      </div>
      <div className="divide-y divide-slate-100">
        {items.map((entry) => (
          <div key={entry.history_id} className="px-5 py-3 flex items-start gap-3">
            <div className="flex-shrink-0 w-8 h-8 rounded-full bg-mck-sky text-mck-blue flex items-center justify-center">
              <UserCog size={14} />
            </div>
            <div className="flex-1 min-w-0">
              <div className="text-sm text-mck-navy">
                <span className="font-medium">{entry.previous_seller_username || 'Unassigned'}</span>
                <span className="text-slate-400 mx-1.5">to</span>
                <span className="font-medium">{entry.new_seller_username || 'Unassigned'}</span>
              </div>
              <div className="text-xs text-slate-500 mt-0.5">
                {entry.change_reason ? <span className="text-mck-orange">{entry.change_reason}</span> : null}
                {entry.change_reason && entry.changed_by_username ? <span className="text-slate-300 mx-1.5">|</span> : null}
                {entry.changed_by_username ? <span>by {entry.changed_by_username}</span> : null}
                <span className="text-slate-300 mx-1.5">|</span>
                <span>{relativeTime(entry.changed_at)}</span>
              </div>
              {entry.notes ? (
                <div className="text-xs text-slate-600 italic mt-1 bg-slate-50 px-2 py-1 rounded">{entry.notes}</div>
              ) : null}
            </div>
          </div>
        ))}
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
