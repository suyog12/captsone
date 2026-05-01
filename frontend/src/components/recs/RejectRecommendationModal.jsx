import { useState, useEffect, useRef } from 'react';
import { useMutation } from '@tanstack/react-query';
import { X, AlertTriangle, Loader2, Check } from 'lucide-react';
import { rejectRecommendation } from '../../api.js';

// Reject recommendation modal

// Seller-only modal that captures why a rec was rejected. Quick-pick chips
// cover the common reasons; "Other" reveals a free-text note. The chosen
// code persists to backend (recommendation_events.outcome='rejected') so
// the team can later analyze rejection patterns by signal, by reason, by
// segment, etc.

const REASONS = [
  { code: 'not_relevant',         label: 'Not relevant to this customer' },
  { code: 'already_have',         label: 'Customer already has this product' },
  { code: 'out_of_stock',         label: 'Out of stock / not available' },
  { code: 'price_too_high',       label: 'Price too high' },
  { code: 'wrong_size_or_spec',   label: 'Wrong size or specification' },
  { code: 'different_brand',      label: 'Customer prefers a different brand' },
  { code: 'bad_timing',           label: 'Bad timing (seasonal / not needed now)' },
  { code: 'wrong_recommendation', label: 'Wrong recommendation (engine error)' },
  { code: 'other',                label: 'Other' }
];

export default function RejectRecommendationModal({
  open,
  onClose,
  rec,
  custId,
  onSuccess
}) {
  const [reasonCode, setReasonCode] = useState(null);
  const [note, setNote] = useState('');
  const dialogRef = useRef(null);

  const mutation = useMutation({
    mutationFn: (payload) => rejectRecommendation(payload),
    onSuccess: (data) => {
      if (onSuccess) onSuccess(rec, data);
      setTimeout(() => onClose(), 600);
    }
  });

  useEffect(() => {
    if (open) {
      setReasonCode(null);
      setNote('');
      mutation.reset();
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    function onKey(e) {
      if (e.key === 'Escape' && !mutation.isPending) onClose();
    }
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, mutation.isPending, onClose]);

  useEffect(() => {
    if (open && dialogRef.current) {
      dialogRef.current.focus();
    }
  }, [open]);

  if (!open || !rec) return null;

  const selected = REASONS.find((r) => r.code === reasonCode);
  const requiresNote = reasonCode === 'other';
  const canSubmit =
    reasonCode !== null &&
    (!requiresNote || note.trim().length >= 3) &&
    !mutation.isPending &&
    !mutation.isSuccess;

  function handleSubmit() {
    if (!canSubmit) return;
    mutation.mutate({
      cust_id: custId,
      item_id: rec.item_id,
      primary_signal: rec.primary_signal,
      rec_purpose: rec.rec_purpose,
      reason_code: reasonCode,
      reason_note: note.trim().length > 0 ? note.trim() : null
    });
  }

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4"
      role="dialog"
      aria-modal="true"
      aria-labelledby="reject-rec-title"
    >
      <div
        className="absolute inset-0 bg-mck-navy/60 backdrop-blur-sm"
        onClick={() => !mutation.isPending && onClose()}
      />

      <div
        ref={dialogRef}
        tabIndex={-1}
        className="relative bg-white rounded-xl shadow-2xl border border-slate-200 w-full max-w-lg max-h-[90vh] flex flex-col overflow-hidden outline-none"
      >
        <div className="px-5 py-4 border-b border-slate-200 flex items-start gap-3">
          <div className="flex-shrink-0 w-9 h-9 rounded-md bg-red-50 flex items-center justify-center">
            <AlertTriangle size={18} className="text-red-600" />
          </div>
          <div className="flex-1 min-w-0">
            <h2 id="reject-rec-title" className="text-base font-semibold text-mck-navy leading-tight">
              Reject this recommendation?
            </h2>
            <div className="text-xs text-slate-500 mt-0.5 truncate">
              {rec.description || `Item ${rec.item_id}`}
            </div>
          </div>
          <button
            type="button"
            onClick={onClose}
            disabled={mutation.isPending}
            className="text-slate-400 hover:text-mck-navy p-1.5 rounded hover:bg-slate-100 disabled:opacity-40 disabled:cursor-not-allowed flex-shrink-0"
            aria-label="Close"
          >
            <X size={16} />
          </button>
        </div>

        <div className="px-5 py-4 flex-1 overflow-y-auto">
          <p className="text-xs text-slate-600 mb-3">
            Pick the reason this recommendation isn't useful. Your feedback
            improves the engine over time.
          </p>

          <div className="grid grid-cols-1 gap-1.5">
            {REASONS.map((r) => {
              const active = r.code === reasonCode;
              return (
                <button
                  key={r.code}
                  type="button"
                  onClick={() => setReasonCode(r.code)}
                  disabled={mutation.isPending || mutation.isSuccess}
                  className={`w-full text-left px-3 py-2 text-sm rounded border transition-all ${
                    active
                      ? 'bg-red-50 text-red-800 border-red-300 ring-2 ring-red-200'
                      : 'bg-white text-slate-700 border-slate-200 hover:border-slate-300'
                  } disabled:opacity-50 disabled:cursor-not-allowed`}
                >
                  <span className="inline-flex items-center gap-2">
                    {active ? <Check size={13} className="flex-shrink-0" /> : null}
                    {r.label}
                  </span>
                </button>
              );
            })}
          </div>

          <div className="mt-4">
            <label className="text-xs font-semibold text-slate-600 uppercase tracking-wider">
              {requiresNote ? 'Tell us more' : 'Add a note (optional)'}
            </label>
            <textarea
              value={note}
              onChange={(e) => setNote(e.target.value)}
              disabled={mutation.isPending || mutation.isSuccess}
              rows={3}
              maxLength={2000}
              placeholder={
                requiresNote
                  ? 'A short description of why this is not useful'
                  : 'Optional details (max 2000 chars)'
              }
              className="mt-1 w-full px-3 py-2 text-sm border border-slate-200 rounded-md focus:border-mck-blue focus:ring-1 focus:ring-mck-blue/20 outline-none disabled:bg-slate-50"
            />
            {requiresNote && note.trim().length < 3 ? (
              <div className="text-[11px] text-red-600 mt-1">
                Please enter at least 3 characters.
              </div>
            ) : null}
          </div>

          {mutation.isError ? (
            <div className="mt-3 text-xs text-red-600 bg-red-50 border border-red-200 rounded px-3 py-2">
              Could not save rejection. Please try again.
            </div>
          ) : null}

          {mutation.isSuccess ? (
            <div className="mt-3 text-xs text-green-700 bg-green-50 border border-green-200 rounded px-3 py-2 flex items-center gap-2">
              <Check size={13} />
              Logged. Thanks for the feedback.
            </div>
          ) : null}
        </div>

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
            disabled={!canSubmit}
            className="inline-flex items-center gap-1.5 px-4 py-2 text-xs font-semibold rounded transition-colors bg-red-600 text-white hover:bg-red-700 disabled:bg-slate-300 disabled:cursor-not-allowed"
          >
            {mutation.isPending ? (
              <>
                <Loader2 size={12} className="animate-spin" />
                Saving...
              </>
            ) : (
              <>
                <AlertTriangle size={12} />
                Reject {selected ? `(${shortLabel(selected.label)})` : ''}
              </>
            )}
          </button>
        </div>
      </div>
    </div>
  );
}

function shortLabel(s) {
  if (!s) return '';
  if (s.length <= 22) return s;
  return s.slice(0, 20) + '...';
}
