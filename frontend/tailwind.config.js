/** @type {import('tailwindcss').Config} */

// Tailwind config

export default {
  content: ['./index.html', './src/**/*.{js,jsx}'],
  theme: {
    extend: {
      fontFamily: {
        sans: ['"Source Sans 3"', 'Source Sans Pro', '-apple-system', 'BlinkMacSystemFont', 'sans-serif']
      },
      colors: {
        // McKesson brand palette
        mck: {
          navy: '#0D2347',
          'navy-deep': '#091830',
          blue: '#1B6BBE',
          'blue-dark': '#155490',
          'blue-light': '#3D8AD9',
          orange: '#F4821F',
          'orange-dark': '#D86A0A',
          'sky': '#EEF5FB'
        },
        // Signal-type accent colors (8 distinct signals)
        signal: {
          peer_gap: '#1B6BBE',
          popularity: '#0EA5E9',
          cart_complement: '#7C3AED',
          item_similarity: '#0F766E',
          replenishment: '#16A34A',
          lapsed_recovery: '#EA580C',
          private_brand_upgrade: '#DC2626',
          medline_conversion: '#9333EA'
        },
        // rec_purpose colors
        purpose: {
          new_product: '#1B6BBE',
          win_back: '#EA580C',
          cross_sell: '#7C3AED',
          mckesson_substitute: '#DC2626',
          replenishment: '#16A34A'
        },
        // lifecycle status colors
        lifecycle: {
          stable_warm: '#16A34A',
          declining_warm: '#EAB308',
          churned_warm: '#DC2626',
          cold_start: '#64748B'
        }
      },
      boxShadow: {
        card: '0 1px 3px rgba(13, 35, 71, 0.06), 0 1px 2px rgba(13, 35, 71, 0.04)',
        'card-hover': '0 4px 12px rgba(13, 35, 71, 0.10), 0 2px 4px rgba(13, 35, 71, 0.06)'
      }
    }
  },
  plugins: []
};
