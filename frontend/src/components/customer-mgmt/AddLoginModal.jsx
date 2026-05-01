// frontend/src/components/customer-mgmt/AddLoginModal.jsx
//
// Modal that lets an admin or seller attach a dashboard login to a
// customer who already has a record but no login. Posts to the new
// POST /users/customers/{cust_id}/login endpoint.

import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { X, UserPlus, AlertCircle, Eye, EyeOff } from 'lucide-react';
import { api } from '../../api.js';

export default function AddLoginModal({ open, onClose, customer, onCreated }) {
  const qc = useQueryClient();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState('');
  const [email, setEmail] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [serverError, setServerError] = useState('');

  const mutation = useMutation({
    mutationFn: async () => {
      const body = {
        username: username.trim(),
        password,
      };
      if (fullName.trim()) body.full_name = fullName.trim();
      if (email.trim()) body.email = email.trim();
      const { data } = await api.post(
        `/users/customers/${customer.cust_id}/login`,
        body
      );
      return data;
    },
    onSuccess: (newUser) => {
      // Invalidate the customer lists so badges refresh
      qc.invalidateQueries({ queryKey: ['admin', 'customers-filter'] });
      qc.invalidateQueries({ queryKey: ['admin', 'customers-search'] });
      qc.invalidateQueries({ queryKey: ['seller', 'all-customers'] });
      qc.invalidateQueries({ queryKey: ['seller', 'me', 'customers'] });
      handleClose();
      if (onCreated) onCreated(newUser);
    },
    onError: (err) => {
      const detail = err?.response?.data?.detail;
      setServerError(typeof detail === 'string' ? detail : 'Could not create login. Please try again.');
    },
  });

  function handleClose() {
    setUsername('');
    setPassword('');
    setFullName('');
    setEmail('');
    setShowPassword(false);
    setServerError('');
    onClose();
  }

  function handleSubmit(e) {
    e.preventDefault();
    setServerError('');
    if (username.trim().length < 3) {
      setServerError('Username must be at least 3 characters.');
      return;
    }
    if (password.length < 6) {
      setServerError('Password must be at least 6 characters.');
      return;
    }
    mutation.mutate();
  }

  if (!open || !customer) return null;

  const displayName = customer.customer_name || `Customer ${customer.cust_id}`;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/50 backdrop-blur-sm p-4">
      <div className="bg-white rounded-lg shadow-xl w-full max-w-md max-h-[90vh] overflow-y-auto">
        <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <UserPlus size={18} className="text-mck-blue" />
            <h2 className="text-base font-semibold text-mck-navy">Add login</h2>
          </div>
          <button
            type="button"
            onClick={handleClose}
            className="text-slate-400 hover:text-slate-600 p-1 rounded hover:bg-slate-100"
          >
            <X size={18} />
          </button>
        </div>

        <div className="px-5 py-4 bg-mck-sky/30 border-b border-slate-100">
          <div className="text-xs text-slate-500 mb-0.5">Creating login for</div>
          <div className="text-sm font-semibold text-mck-navy">{displayName}</div>
          <div className="text-xs text-slate-500 mt-0.5">
            #{customer.cust_id}
            {customer.segment ? ` | ${customer.segment}` : ''}
            {customer.specialty_code ? ` | Specialty ${customer.specialty_code}` : ''}
          </div>
        </div>

        <form onSubmit={handleSubmit} className="px-5 py-4 space-y-3">
          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1">
              Username <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="e.g. customer_33741"
              required
              minLength={3}
              maxLength={100}
              className="w-full px-3 py-2 text-sm border border-slate-200 rounded-md focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue"
            />
          </div>

          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1">
              Password <span className="text-red-500">*</span>
            </label>
            <div className="relative">
              <input
                type={showPassword ? 'text' : 'password'}
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="Min 6 characters"
                required
                minLength={6}
                maxLength={200}
                className="w-full px-3 py-2 pr-9 text-sm border border-slate-200 rounded-md focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue"
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600 p-1"
              >
                {showPassword ? <EyeOff size={14} /> : <Eye size={14} />}
              </button>
            </div>
            <div className="text-[11px] text-slate-500 mt-1">
              Share this temp password with the customer. They can change it after first login.
            </div>
          </div>

          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1">
              Full name <span className="text-slate-400 font-normal">(optional)</span>
            </label>
            <input
              type="text"
              value={fullName}
              onChange={(e) => setFullName(e.target.value)}
              placeholder="Contact's name"
              maxLength={200}
              className="w-full px-3 py-2 text-sm border border-slate-200 rounded-md focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue"
            />
          </div>

          <div>
            <label className="block text-xs font-semibold text-slate-700 mb-1">
              Email <span className="text-slate-400 font-normal">(optional)</span>
            </label>
            <input
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="contact@example.com"
              maxLength={200}
              className="w-full px-3 py-2 text-sm border border-slate-200 rounded-md focus:outline-none focus:ring-2 focus:ring-mck-blue focus:border-mck-blue"
            />
          </div>

          {serverError ? (
            <div className="flex items-start gap-2 text-xs text-red-700 bg-red-50 border border-red-200 rounded p-2">
              <AlertCircle size={14} className="flex-shrink-0 mt-0.5" />
              <span>{serverError}</span>
            </div>
          ) : null}
        </form>

        <div className="px-5 py-3 border-t border-slate-100 flex items-center justify-end gap-2">
          <button
            type="button"
            onClick={handleClose}
            disabled={mutation.isPending}
            className="text-xs font-medium text-slate-600 hover:text-mck-navy px-3 py-1.5 rounded hover:bg-slate-50 disabled:text-slate-300"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={handleSubmit}
            disabled={mutation.isPending || username.trim().length < 3 || password.length < 6}
            className="text-xs font-semibold bg-mck-blue text-white px-4 py-1.5 rounded hover:bg-mck-blue-dark disabled:bg-slate-300 disabled:cursor-not-allowed"
          >
            {mutation.isPending ? 'Creating...' : 'Create login'}
          </button>
        </div>
      </div>
    </div>
  );
}
