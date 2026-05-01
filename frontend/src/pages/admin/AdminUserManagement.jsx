import { useState, useMemo } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  Users,
  Search,
  Plus,
  X,
  Check,
  UserPlus,
  ShieldCheck,
  Briefcase,
  Building2,
  Loader2,
  Power,
  PowerOff,
  ChevronDown,
  ChevronUp
} from 'lucide-react';
import AppShell from '../../components/shell/AppShell.jsx';
import Card from '../../components/ui/Card.jsx';
import StatCard from '../../components/ui/StatCard.jsx';
import SegmentedControl from '../../components/ui/SegmentedControl.jsx';
import { FullPanelSpinner } from '../../components/ui/Spinner.jsx';
import EmptyState from '../../components/ui/EmptyState.jsx';
import {
  listUsers,
  createAdmin,
  createSeller,
  createCustomer,
  deactivateUser,
  reactivateUser
} from '../../api.js';
import { formatDate, relativeTime } from '../../lib/format.js';
import {
  marketOptions,
  sizeOptions,
  specialtyOptions
} from '../../lib/customerLabels.js';

// Admin user management

const ROLE_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'admin', label: 'Admins' },
  { value: 'seller', label: 'Sellers' },
  { value: 'customer', label: 'Customers' }
];

const STATUS_OPTIONS = [
  { value: 'all', label: 'All' },
  { value: 'active', label: 'Active' },
  { value: 'inactive', label: 'Inactive' }
];

export default function AdminUserManagement() {
  const [roleFilter, setRoleFilter] = useState('all');
  const [statusFilter, setStatusFilter] = useState('all');
  const [search, setSearch] = useState('');
  const [createModalOpen, setCreateModalOpen] = useState(0);
  const [createRole, setCreateRole] = useState('seller');
  const [expanded, setExpanded] = useState(0);

  const usersQuery = useQuery({
    queryKey: ['admin', 'users-all'],
    queryFn: () => listUsers({ limit: 500, offset: 0 })
  });

  const users = useMemo(() => (usersQuery.data && usersQuery.data.items) || [], [usersQuery.data]);

  const counts = useMemo(() => {
    const c = { all: users.length, admin: 0, seller: 0, customer: 0, active: 0, inactive: 0 };
    users.forEach((u) => {
      if (u.role && c[u.role] !== undefined) c[u.role] += 1;
      if (u.is_active === true || u.is_active === 1) c.active += 1;
      else c.inactive += 1;
    });
    return c;
  }, [users]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return users.filter((u) => {
      if (roleFilter !== 'all' && u.role !== roleFilter) return 0;
      const isActive = u.is_active === true || u.is_active === 1;
      if (statusFilter === 'active' && !isActive) return 0;
      if (statusFilter === 'inactive' && isActive) return 0;
      if (!q) return 1;
      const hay = `${u.username || ''} ${u.full_name || ''} ${u.email || ''}`.toLowerCase();
      return hay.indexOf(q) !== -1 ? 1 : 0;
    });
  }, [users, roleFilter, statusFilter, search]);

  const INITIAL = 15;
  const visible = expanded === 1 ? filtered : filtered.slice(0, INITIAL);
  const hidden = filtered.length - INITIAL;

  function openCreate(role) {
    setCreateRole(role);
    setCreateModalOpen(1);
  }

  return (
    <AppShell title="User management" subtitle="Admins, sellers, and customers">
      <div className="space-y-6">
        {/* KPI strip */}
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
          <StatCard label="Total users" value={counts.all} icon={Users} accent="mck-navy" />
          <StatCard label="Admins" value={counts.admin} icon={ShieldCheck} accent="mck-blue" />
          <StatCard label="Sellers" value={counts.seller} icon={Briefcase} accent="mck-orange" />
          <StatCard label="Customers" value={counts.customer} icon={Building2} accent="green" />
          <StatCard label="Active" value={counts.active} hint={`${counts.inactive} inactive`} icon={Power} accent="slate" />
        </div>

        {/* Quick-create buttons */}
        <Card>
          <div className="flex items-center justify-between flex-wrap gap-3">
            <div>
              <h3 className="text-sm font-semibold text-mck-navy">Create user</h3>
              <p className="text-xs text-slate-500 mt-0.5">Provision a new admin, seller, or customer account</p>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <button
                type="button"
                onClick={() => openCreate('admin')}
                className="inline-flex items-center gap-1.5 text-xs font-semibold text-white bg-mck-blue hover:bg-mck-blue-dark px-3 py-1.5 rounded"
              >
                <Plus size={12} />
                New admin
              </button>
              <button
                type="button"
                onClick={() => openCreate('seller')}
                className="inline-flex items-center gap-1.5 text-xs font-semibold text-white bg-mck-orange hover:bg-mck-orange-dark px-3 py-1.5 rounded"
              >
                <Plus size={12} />
                New seller
              </button>
              <button
                type="button"
                onClick={() => openCreate('customer')}
                className="inline-flex items-center gap-1.5 text-xs font-semibold text-white bg-mck-navy hover:bg-mck-navy/80 px-3 py-1.5 rounded"
              >
                <Plus size={12} />
                New customer
              </button>
            </div>
          </div>
        </Card>

        {/* Filter bar */}
        <Card padding="none">
          <div className="px-5 pt-5 pb-3 flex items-center justify-between gap-3 flex-wrap">
            <div>
              <h3 className="text-sm font-semibold text-mck-navy">All users</h3>
              <p className="text-xs text-slate-500 mt-0.5">
                {filtered.length} of {users.length}
                {expanded === 0 && filtered.length > INITIAL ? <> &middot; showing top {INITIAL}</> : null}
              </p>
            </div>
            <div className="flex items-center gap-2 flex-wrap">
              <div className="relative">
                <Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400 pointer-events-none" />
                <input
                  type="text"
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  placeholder="Search by name, username, email"
                  className="pl-8 pr-3 py-1.5 text-xs border border-slate-200 rounded-md focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue placeholder:text-slate-400 w-56"
                />
              </div>
              <SegmentedControl options={ROLE_OPTIONS} value={roleFilter} onChange={setRoleFilter} />
              <SegmentedControl options={STATUS_OPTIONS} value={statusFilter} onChange={setStatusFilter} />
            </div>
          </div>

          {usersQuery.isLoading ? (
            <FullPanelSpinner label="Loading users" />
          ) : usersQuery.isError ? (
            <div className="px-5 pb-6">
              <EmptyState title="Could not load users" />
            </div>
          ) : filtered.length === 0 ? (
            <div className="px-5 pb-6">
              <EmptyState
                icon={Users}
                title="No users match the filters"
                description="Try clearing the search or filter."
              />
            </div>
          ) : (
            <div className="border-t border-slate-100">
              <div className="divide-y divide-slate-100">
                {visible.map((u) => (
                  <UserRow key={u.user_id} user={u} />
                ))}
              </div>
              {filtered.length > INITIAL ? (
                <div className="border-t border-slate-100">
                  <button
                    type="button"
                    onClick={() => setExpanded(expanded === 1 ? 0 : 1)}
                    className="w-full px-5 py-2.5 text-xs font-semibold text-mck-blue hover:text-mck-blue-dark hover:bg-mck-sky/40 transition-colors flex items-center justify-center gap-1.5"
                  >
                    {expanded === 1 ? (
                      <>
                        <ChevronUp size={14} />
                        Show less
                      </>
                    ) : (
                      <>
                        <ChevronDown size={14} />
                        Show all {filtered.length} users ({hidden} more)
                      </>
                    )}
                  </button>
                </div>
              ) : null}
            </div>
          )}
        </Card>
      </div>

      {createModalOpen === 1 ? (
        <CreateUserModal role={createRole} onClose={() => setCreateModalOpen(0)} />
      ) : null}
    </AppShell>
  );
}

function UserRow({ user }) {
  const queryClient = useQueryClient();
  const isActive = user.is_active === true || user.is_active === 1;

  const deactivateMutation = useMutation({
    mutationFn: () => deactivateUser(user.user_id),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['admin', 'users-all'] })
  });

  const reactivateMutation = useMutation({
    mutationFn: () => reactivateUser(user.user_id),
    onSettled: () => queryClient.invalidateQueries({ queryKey: ['admin', 'users-all'] })
  });

  const isPending = deactivateMutation.isPending || reactivateMutation.isPending;

  return (
    <div className="px-5 py-3 flex items-center gap-4 hover:bg-slate-50/50">
      <div className={`flex-shrink-0 w-9 h-9 rounded-full text-white font-semibold text-xs flex items-center justify-center ${roleAvatarColor(user.role)}`}>
        {initials(user)}
      </div>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 flex-wrap">
          <span className="text-sm font-semibold text-mck-navy truncate">
            {user.full_name || user.username}
          </span>
          <span className="text-[10px] uppercase tracking-wider text-slate-400">@{user.username}</span>
          <RoleBadge role={user.role} />
          {isActive ? (
            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-green-700 bg-green-50 border border-green-200 px-1.5 py-0.5 rounded">
              <span className="w-1 h-1 rounded-full bg-green-500" />
              Active
            </span>
          ) : (
            <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-slate-500 bg-slate-100 border border-slate-200 px-1.5 py-0.5 rounded">
              Inactive
            </span>
          )}
        </div>
        <div className="text-xs text-slate-500 mt-0.5 flex items-center gap-x-2 gap-y-0.5 flex-wrap">
          {user.email ? <span className="truncate">{user.email}</span> : null}
          {user.cust_id ? (
            <>
              <span className="text-slate-300">|</span>
              <span>Cust #{user.cust_id}</span>
            </>
          ) : null}
          <span className="text-slate-300">|</span>
          <span>Created {formatDate(user.created_at)}</span>
          <span className="text-slate-300">|</span>
          <span>Last login {user.last_login_at ? relativeTime(user.last_login_at) : 'never'}</span>
        </div>
      </div>

      <div className="flex-shrink-0">
        {isActive ? (
          <button
            type="button"
            onClick={() => deactivateMutation.mutate()}
            disabled={isPending}
            className="inline-flex items-center gap-1 text-xs font-medium text-red-700 hover:text-red-800 hover:bg-red-50 disabled:text-slate-300 disabled:cursor-not-allowed px-2.5 py-1.5 rounded"
          >
            {isPending ? <Loader2 size={12} className="animate-spin" /> : <PowerOff size={12} />}
            Deactivate
          </button>
        ) : (
          <button
            type="button"
            onClick={() => reactivateMutation.mutate()}
            disabled={isPending}
            className="inline-flex items-center gap-1 text-xs font-medium text-green-700 hover:text-green-800 hover:bg-green-50 disabled:text-slate-300 disabled:cursor-not-allowed px-2.5 py-1.5 rounded"
          >
            {isPending ? <Loader2 size={12} className="animate-spin" /> : <Power size={12} />}
            Reactivate
          </button>
        )}
      </div>
    </div>
  );
}

function CreateUserModal({ role, onClose }) {
  const queryClient = useQueryClient();
  const [form, setForm] = useState({
    username: '',
    password: '',
    full_name: '',
    email: '',
    customer_business_name: '',
    market_code: 'PO',
    size_tier: 'new',
    specialty_code: ''
  });

  const createMutation = useMutation({
    mutationFn: () => {
      if (role === 'admin') {
        return createAdmin({
          username: form.username,
          password: form.password,
          full_name: form.full_name,
          email: form.email
        });
      }
      if (role === 'seller') {
        return createSeller({
          username: form.username,
          password: form.password,
          full_name: form.full_name,
          email: form.email
        });
      }
      return createCustomer({
        username: form.username,
        password: form.password,
        full_name: form.full_name,
        email: form.email,
        customer_business_name: form.customer_business_name,
        market_code: form.market_code,
        size_tier: form.size_tier,
        specialty_code: form.specialty_code
      });
    },
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['admin', 'users-all'] });
      onClose();
    }
  });

  function update(field, value) {
    setForm({ ...form, [field]: value });
  }

  function handleSubmit(e) {
    e.preventDefault();
    createMutation.mutate();
  }

  const title = role === 'admin' ? 'Create admin' : role === 'seller' ? 'Create seller' : 'Create customer';

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-white rounded-lg shadow-xl max-w-md w-full max-h-[90vh] overflow-auto">
        <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between sticky top-0 bg-white z-10">
          <div className="flex items-center gap-2">
            <UserPlus size={16} className="text-mck-blue" />
            <h3 className="text-base font-semibold text-mck-navy">{title}</h3>
          </div>
          <button type="button" onClick={onClose} className="text-slate-400 hover:text-slate-600 p-1 rounded hover:bg-slate-100">
            <X size={16} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="px-5 py-4 space-y-3">
          <FormField label="Username" required>
            <input type="text" value={form.username} onChange={(e) => update('username', e.target.value)} required className={fieldClass} />
          </FormField>
          <FormField label="Password" required>
            <input type="password" value={form.password} onChange={(e) => update('password', e.target.value)} required minLength={8} className={fieldClass} />
          </FormField>
          <FormField label="Full name" required>
            <input type="text" value={form.full_name} onChange={(e) => update('full_name', e.target.value)} required className={fieldClass} />
          </FormField>
          <FormField label="Email" required>
            <input type="email" value={form.email} onChange={(e) => update('email', e.target.value)} required className={fieldClass} />
          </FormField>

          {role === 'customer' ? (
            <>
              <FormField label="Customer business name" required>
                <input type="text" value={form.customer_business_name} onChange={(e) => update('customer_business_name', e.target.value)} required className={fieldClass} />
              </FormField>
              <div className="grid grid-cols-2 gap-3">
                <FormField label="Market code">
                  <select value={form.market_code} onChange={(e) => update('market_code', e.target.value)} className={fieldClass}>
                    {marketOptions().map((m) => (
                      <option key={m.code} value={m.code}>
                        {m.label} ({m.code})
                      </option>
                    ))}
                  </select>
                </FormField>
                <FormField label="Size tier">
                  <select value={form.size_tier} onChange={(e) => update('size_tier', e.target.value)} className={fieldClass}>
                    {sizeOptions().map((s) => (
                      <option key={s.code} value={s.code}>
                        {s.label}
                      </option>
                    ))}
                  </select>
                </FormField>
              </div>
              <FormField label="Specialty code">
                <select value={form.specialty_code} onChange={(e) => update('specialty_code', e.target.value)} className={fieldClass}>
                  <option value="">Not specified</option>
                  {specialtyOptions().map((s) => (
                    <option key={s.code} value={s.code}>
                      {s.label} ({s.code})
                    </option>
                  ))}
                </select>
              </FormField>
            </>
          ) : null}

          {createMutation.isError ? (
            <div className="bg-red-50 border border-red-200 rounded-md px-3 py-2 text-xs text-red-700">
              {(createMutation.error && createMutation.error.response && createMutation.error.response.data && createMutation.error.response.data.detail) || 'Could not create user.'}
            </div>
          ) : null}

          <div className="flex justify-end gap-2 pt-2 border-t border-slate-100">
            <button type="button" onClick={onClose} className="px-3 py-1.5 text-sm text-slate-600 hover:text-mck-navy rounded hover:bg-slate-100">
              Cancel
            </button>
            <button
              type="submit"
              disabled={createMutation.isPending}
              className="inline-flex items-center gap-1.5 px-3 py-1.5 text-sm font-semibold text-white bg-mck-blue hover:bg-mck-blue-dark disabled:bg-slate-300 rounded"
            >
              {createMutation.isPending ? <Loader2 size={13} className="animate-spin" /> : <Check size={13} />}
              Create
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

const fieldClass = 'mt-1 w-full text-sm border border-slate-200 rounded-md px-3 py-2 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue placeholder:text-slate-400';

function FormField({ label, required, children }) {
  return (
    <label className="block">
      <span className="text-xs font-semibold text-slate-600 uppercase tracking-wider">
        {label}
        {required ? <span className="text-mck-orange ml-0.5">*</span> : null}
      </span>
      {children}
    </label>
  );
}

function RoleBadge({ role }) {
  const map = {
    admin: { label: 'Admin', cls: 'text-blue-700 bg-blue-50 border-blue-200' },
    seller: { label: 'Seller', cls: 'text-orange-700 bg-orange-50 border-orange-200' },
    customer: { label: 'Customer', cls: 'text-slate-700 bg-slate-100 border-slate-200' }
  };
  const cfg = map[role] || { label: role || 'Unknown', cls: 'text-slate-700 bg-slate-100 border-slate-200' };
  return (
    <span className={`inline-flex items-center text-[10px] font-semibold px-1.5 py-0.5 rounded border ${cfg.cls}`}>
      {cfg.label}
    </span>
  );
}

function roleAvatarColor(role) {
  if (role === 'admin') return 'bg-mck-blue';
  if (role === 'seller') return 'bg-mck-orange';
  return 'bg-slate-500';
}

function initials(user) {
  if (!user) return '?';
  if (user.full_name) {
    const parts = user.full_name.trim().split(/\s+/);
    if (parts.length >= 2) return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
    return user.full_name.slice(0, 2).toUpperCase();
  }
  return (user.username || '?').slice(0, 2).toUpperCase();
}
