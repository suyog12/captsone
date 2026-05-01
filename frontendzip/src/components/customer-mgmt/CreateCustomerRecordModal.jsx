import { useState, useEffect, useRef } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { UserPlus, X, Check, Loader2, Info } from 'lucide-react';
import { createCustomerRecord } from '../../api.js';
import {
  marketOptions,
  sizeOptions,
  specialtyOptions
} from '../../lib/customerLabels.js';

// Create customer record modal

// Used by sellers (and optionally admins) to add a new customer account
// without creating a customer login. The Customer record is created in
// recdash.customers and auto-assigned to the calling seller. Admins can
// assign to any seller via assigned_seller_id, but for now this modal
// uses the seller-default flow (auto-assign to the caller).
//
// Fields:
//   - Customer business name (required)
//   - Market vertical (dropdown)
//   - Size tier (dropdown)
//   - Specialty (dropdown, optional)
//
// Login fields are intentionally absent. If the customer needs a login
// later, an admin can create one via AdminUserManagement.

export default function CreateCustomerRecordModal({ open, onClose, onCreated }) {
  const queryClient = useQueryClient();
  const dialogRef = useRef(null);
  const firstFieldRef = useRef(null);

  const [form, setForm] = useState({
    customer_business_name: '',
    market_code: 'PO',
    size_tier: 'small',
    specialty_code: ''
  });

  const mutation = useMutation({
    mutationFn: (payload) => createCustomerRecord(payload),
    onSuccess: (data) => {
      // Refresh anywhere a customer roster might be cached
      queryClient.invalidateQueries({ queryKey: ['seller', 'me', 'customers'] });
      queryClient.invalidateQueries({ queryKey: ['seller', 'all-customers'] });
      queryClient.invalidateQueries({ queryKey: ['admin', 'customers'] });
      queryClient.invalidateQueries({ queryKey: ['customers', 'filter'] });
      if (onCreated) onCreated(data);
      // Close after a brief beat so the success state is visible
      setTimeout(() => {
        onClose();
      }, 700);
    }
  });

  // Reset form when reopened
  useEffect(() => {
    if (open) {
      setForm({
        customer_business_name: '',
        market_code: 'PO',
        size_tier: 'small',
        specialty_code: ''
      });
      mutation.reset();
    }
  }, [open]);

  // Esc to close
  useEffect(() => {
    if (!open) return;
    function onKey(e) {
      if (e.key === 'Escape' && !mutation.isPending) onClose();
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, mutation.isPending, onClose]);

  // Focus first field on open
  useEffect(() => {
    if (open && firstFieldRef.current) {
      // Small delay so the modal has mounted before focus
      const t = setTimeout(() => firstFieldRef.current.focus(), 50);
      return () => clearTimeout(t);
    }
  }, [open]);

  if (!open) return null;

  function update(field, value) {
    setForm({ ...form, [field]: value });
  }

  function handleSubmit(e) {
    e.preventDefault();
    if (!form.customer_business_name.trim()) return;
    mutation.mutate({
      customer_business_name: form.customer_business_name.trim(),
      market_code: form.market_code,
      size_tier: form.size_tier,
      specialty_code: form.specialty_code || null
    });
  }

  const markets = marketOptions();
  const sizes = sizeOptions();
  const specialties = specialtyOptions();

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="create-customer-record-title"
    >
      <div
        className="absolute inset-0 bg-mck-navy/60 backdrop-blur-sm"
        onClick={() => !mutation.isPending && onClose()}
      />

      <div
        ref={dialogRef}
        tabIndex={-1}
        className="relative bg-white rounded-xl shadow-2xl border border-slate-200 w-full max-w-md max-h-[90vh] flex flex-col overflow-hidden outline-none"
      >
        <div className="px-5 py-4 border-b border-slate-200 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <UserPlus size={16} className="text-mck-blue" />
            <h2 id="create-customer-record-title" className="text-base font-semibold text-mck-navy">
              Add a customer
            </h2>
          </div>
          <button
            type="button"
            onClick={onClose}
            disabled={mutation.isPending}
            className="text-slate-400 hover:text-mck-navy p-1.5 rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed"
            aria-label="Close"
          >
            <X size={16} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="px-5 py-4 flex-1 overflow-y-auto space-y-4">
          <div className="bg-slate-50 border border-slate-200 rounded-md px-3 py-2 flex items-start gap-2">
            <Info size={13} className="text-mck-blue mt-0.5 flex-shrink-0" />
            <p className="text-[11px] text-slate-600 leading-snug">
              Creates the customer record only. The new customer is automatically
              assigned to you. A login can be added later by an administrator.
            </p>
          </div>

          <FormField label="Customer business name" required>
            <input
              ref={firstFieldRef}
              type="text"
              value={form.customer_business_name}
              onChange={(e) => update('customer_business_name', e.target.value)}
              required
              minLength={2}
              maxLength={200}
              placeholder="e.g. Sunrise Pediatrics"
              className={fieldClass}
              disabled={mutation.isPending || mutation.isSuccess}
            />
          </FormField>

          <div className="grid grid-cols-2 gap-3">
            <FormField label="Market vertical" required>
              <select
                value={form.market_code}
                onChange={(e) => update('market_code', e.target.value)}
                className={fieldClass}
                disabled={mutation.isPending || mutation.isSuccess}
              >
                {markets.map((m) => (
                  <option key={m.code} value={m.code}>
                    {m.label} ({m.code})
                  </option>
                ))}
              </select>
            </FormField>
            <FormField label="Size tier" required>
              <select
                value={form.size_tier}
                onChange={(e) => update('size_tier', e.target.value)}
                className={fieldClass}
                disabled={mutation.isPending || mutation.isSuccess}
              >
                {sizes.map((s) => (
                  <option key={s.code} value={s.code}>
                    {s.label}
                  </option>
                ))}
              </select>
            </FormField>
          </div>

          <FormField label="Provider specialty">
            <select
              value={form.specialty_code}
              onChange={(e) => update('specialty_code', e.target.value)}
              className={fieldClass}
              disabled={mutation.isPending || mutation.isSuccess}
            >
              <option value="">Not specified</option>
              {specialties.map((s) => (
                <option key={s.code} value={s.code}>
                  {s.label} ({s.code})
                </option>
              ))}
            </select>
          </FormField>

          {mutation.isError ? (
            <div className="bg-red-50 border border-red-200 rounded-md px-3 py-2 text-xs text-red-700">
              {(mutation.error
                && mutation.error.response
                && mutation.error.response.data
                && mutation.error.response.data.detail)
                || 'Could not create customer. Please try again.'}
            </div>
          ) : null}

          {mutation.isSuccess ? (
            <div className="bg-green-50 border border-green-200 rounded-md px-3 py-2 text-xs text-green-700 flex items-center gap-2">
              <Check size={13} />
              Customer created and assigned to you.
            </div>
          ) : null}
        </form>

        <div className="px-5 py-3 border-t border-slate-200 bg-slate-50 flex items-center justify-end gap-2">
          <button
            type="button"
            onClick={onClose}
            disabled={mutation.isPending}
            className="px-3 py-1.5 text-xs font-medium text-slate-600 hover:bg-slate-100 rounded disabled:opacity-40"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSubmit}
            disabled={
              mutation.isPending
              || mutation.isSuccess
              || !form.customer_business_name.trim()
            }
            className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded transition-colors bg-mck-blue text-white hover:bg-mck-blue-dark disabled:bg-slate-300 disabled:cursor-not-allowed"
          >
            {mutation.isPending ? (
              <>
                <Loader2 size={12} className="animate-spin" />
                Creating...
              </>
            ) : mutation.isSuccess ? (
              <>
                <Check size={12} />
                Created
              </>
            ) : (
              <>
                <UserPlus size={12} />
                Create customer
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

const fieldClass =
  'mt-1 w-full text-sm border border-slate-200 rounded-md px-3 py-2 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue placeholder:text-slate-400 disabled:bg-slate-50 disabled:text-slate-400';

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
