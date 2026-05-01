import { Quote } from 'lucide-react';

// Pitch reason

// The pitch_reason field is the single most important UX element on a
// recommendation card. The API returns sentences like:
//   "Customers who buy your products also buy this. Common pairing in
//    Lab-Non-Waived Lab. Aligns with your specialty (M07)."
// The seller uses this verbatim when talking to the customer, so we
// display it prominently with no transformation.

export default function PitchReason({ text, variant = 'card', className = '' }) {
  if (!text) return null;

  if (variant === 'inline') {
    return (
      <p className={`text-sm text-slate-700 italic leading-relaxed ${className}`}>
        <Quote size={14} className="inline mr-1.5 text-mck-orange" />
        {text}
      </p>
    );
  }

  // Default: prominent card variant with orange left stripe
  return (
    <div className={`flex gap-3 bg-orange-50/60 border-l-4 border-mck-orange rounded-r-md px-4 py-3 ${className}`}>
      <Quote size={18} className="text-mck-orange flex-shrink-0 mt-0.5" />
      <p className="text-sm text-mck-navy leading-relaxed">{text}</p>
    </div>
  );
}
