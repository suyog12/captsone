import { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { useNavigate } from 'react-router-dom';
import { Lock, Check, AlertCircle, Loader2, Eye, EyeOff, ArrowLeft } from 'lucide-react';
import AppShell from '../components/shell/AppShell.jsx';
import Card from '../components/ui/Card.jsx';
import { changeMyPassword } from '../api.js';

// Change password page

// Self-service password change for any logged-in user. Calls
// PATCH /users/me/password which requires the current password and
// re-issues no token (the existing JWT stays valid until it expires).
//
// Validation:
//   - current password required
//   - new password >= 8 chars
//   - confirm matches new
//   - new differs from current

export default function ChangePasswordPage() {
  const navigate = useNavigate();
  const [current, setCurrent] = useState('');
  const [next, setNext] = useState('');
  const [confirm, setConfirm] = useState('');
  const [showCurrent, setShowCurrent] = useState(0);
  const [showNext, setShowNext] = useState(0);
  const [clientError, setClientError] = useState(null);

  const mutation = useMutation({
    mutationFn: () => changeMyPassword(current, next),
    onSuccess: () => {
      // Clear inputs after success - user can navigate away whenever
      setCurrent('');
      setNext('');
      setConfirm('');
      setClientError(null);
    }
  });

  function handleSubmit(e) {
    e.preventDefault();
    setClientError(null);

    if (!current) {
      setClientError('Enter your current password.');
      return;
    }
    if (next.length < 8) {
      setClientError('New password must be at least 8 characters.');
      return;
    }
    if (next === current) {
      setClientError('New password must be different from your current password.');
      return;
    }
    if (next !== confirm) {
      setClientError('New password and confirmation do not match.');
      return;
    }

    mutation.mutate();
  }

  const serverError =
    (mutation.error
      && mutation.error.response
      && mutation.error.response.data
      && mutation.error.response.data.detail)
    || (mutation.isError ? 'Could not update password. Please try again.' : null);

  const errorMsg = clientError || serverError;

  return (
    <AppShell
      title="Change password"
      subtitle="Update the password used to log in to this account"
      actions={
        <button
          type="button"
          onClick={() => navigate(-1)}
          className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-600 hover:text-mck-navy px-2 py-1 rounded hover:bg-slate-100"
        >
          <ArrowLeft size={14} />
          Back
        </button>
      }
    >
      <div className="max-w-md">
        <Card padding="lg">
          <div className="flex items-center gap-2 mb-4">
            <div className="w-9 h-9 rounded-md bg-mck-sky flex items-center justify-center">
              <Lock size={16} className="text-mck-blue" />
            </div>
            <div>
              <h2 className="text-base font-semibold text-mck-navy">Update password</h2>
              <p className="text-xs text-slate-500">
                Choose a new password. You will stay signed in.
              </p>
            </div>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <PasswordField
              label="Current password"
              value={current}
              onChange={setCurrent}
              show={showCurrent === 1}
              onToggleShow={() => setShowCurrent(showCurrent === 1 ? 0 : 1)}
              autoComplete="current-password"
              disabled={mutation.isPending || mutation.isSuccess}
            />

            <PasswordField
              label="New password"
              value={next}
              onChange={setNext}
              show={showNext === 1}
              onToggleShow={() => setShowNext(showNext === 1 ? 0 : 1)}
              autoComplete="new-password"
              hint="At least 8 characters"
              disabled={mutation.isPending || mutation.isSuccess}
            />

            <PasswordField
              label="Confirm new password"
              value={confirm}
              onChange={setConfirm}
              show={showNext === 1}
              onToggleShow={() => setShowNext(showNext === 1 ? 0 : 1)}
              autoComplete="new-password"
              disabled={mutation.isPending || mutation.isSuccess}
            />

            {errorMsg ? (
              <div className="flex items-start gap-2 bg-red-50 border border-red-200 rounded-md px-3 py-2 text-xs text-red-700">
                <AlertCircle size={13} className="flex-shrink-0 mt-0.5" />
                <span>{errorMsg}</span>
              </div>
            ) : null}

            {mutation.isSuccess ? (
              <div className="flex items-start gap-2 bg-green-50 border border-green-200 rounded-md px-3 py-2 text-xs text-green-700">
                <Check size={13} className="flex-shrink-0 mt-0.5" />
                <span>
                  Password updated. Use the new password the next time you sign in.
                </span>
              </div>
            ) : null}

            <div className="flex items-center justify-end gap-2 pt-2">
              <button
                type="button"
                onClick={() => navigate(-1)}
                disabled={mutation.isPending}
                className="px-3 py-1.5 text-sm text-slate-600 hover:text-mck-navy rounded hover:bg-slate-100 disabled:opacity-40"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={
                  mutation.isPending
                  || (!current || !next || !confirm)
                }
                className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-semibold rounded transition-colors bg-mck-blue text-white hover:bg-mck-blue-dark disabled:bg-slate-300 disabled:cursor-not-allowed"
              >
                {mutation.isPending ? (
                  <>
                    <Loader2 size={13} className="animate-spin" />
                    Updating...
                  </>
                ) : (
                  <>
                    <Lock size={13} />
                    Update password
                  </>
                )}
              </button>
            </div>
          </form>
        </Card>
      </div>
    </AppShell>
  );
}

function PasswordField({
  label,
  value,
  onChange,
  show,
  onToggleShow,
  autoComplete,
  hint,
  disabled
}) {
  return (
    <label className="block">
      <span className="text-xs font-semibold text-slate-600 uppercase tracking-wider">
        {label}
      </span>
      <div className="relative mt-1">
        <input
          type={show ? 'text' : 'password'}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          autoComplete={autoComplete}
          disabled={disabled}
          className="w-full text-sm border border-slate-200 rounded-md pl-3 pr-10 py-2 text-mck-navy focus:outline-none focus:ring-2 focus:ring-mck-blue placeholder:text-slate-400 disabled:bg-slate-50 disabled:text-slate-400"
        />
        <button
          type="button"
          onClick={onToggleShow}
          tabIndex={-1}
          aria-label={show ? 'Hide password' : 'Show password'}
          className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-400 hover:text-mck-navy p-1 rounded"
        >
          {show ? <EyeOff size={14} /> : <Eye size={14} />}
        </button>
      </div>
      {hint ? <span className="text-[11px] text-slate-500 mt-1 block">{hint}</span> : null}
    </label>
  );
}
