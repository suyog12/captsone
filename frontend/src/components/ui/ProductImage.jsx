import { useState } from 'react';

// Product image

// Renders a product image with a McKesson-branded placeholder fallback.
// Today the API does not return image URLs, so every product shows the
// placeholder. The component already accepts an imageUrl prop so when
// real images are available later, we wire one prop in one component
// and every surface picks it up.

const PLACEHOLDER = '/mckesson-product-placeholder.svg';

const SIZES = {
  xs: 'w-10 h-10',
  sm: 'w-14 h-14',
  md: 'w-20 h-20',
  lg: 'w-28 h-28',
  xl: 'w-40 h-40'
};

export default function ProductImage({ imageUrl, alt = 'Product', size = 'md', rounded = 'md', className = '' }) {
  const [errored, setErrored] = useState(0);
  const src = imageUrl && errored === 0 ? imageUrl : PLACEHOLDER;
  const sizeClass = SIZES[size] || SIZES.md;
  const roundedClass = rounded === 'full' ? 'rounded-full' : rounded === 'lg' ? 'rounded-lg' : 'rounded-md';

  return (
    <div
      className={`${sizeClass} ${roundedClass} bg-mck-sky border border-slate-200 overflow-hidden flex-shrink-0 flex items-center justify-center ${className}`}
    >
      <img
        src={src}
        alt={alt}
        loading="lazy"
        onError={() => setErrored(1)}
        className="w-full h-full object-contain"
      />
    </div>
  );
}
