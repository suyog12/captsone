// Customer code labels

// Translates raw enum codes from the customer record into human-readable
// labels for display. The codes themselves stay in the database and API
// responses; this module is a frontend-only display layer.
//
// Coverage as of demo data:
//   - segment        : market_size_tier (e.g. PO_large, IDN_acute)
//   - market_code    : top-level market vertical (PO, AC, LTC, ...)
//   - specialty_code : provider specialty (FP, IM, PD, ...)
//   - archetype      : derived customer-business type
//   - supplier_profile : distribution profile (mixed, dedicated, ...)
//   - status         : lifecycle (new, stable, lapsing, churned, ...)
//   - size_tier      : derived size from segment suffix

// Market verticals - the prefix in segment codes like PO_large
const MARKET_LABELS = {
  PO: 'Physician Office',
  AC: 'Hospital / Acute Care',
  IDN: 'Integrated Delivery Network',
  LTC: 'Long-Term Care',
  HC: 'Home Care',
  LC: 'Lab / Diagnostic',
  SC: 'Surgery Center',
  GOVT: 'Government / Military',
  EDU: 'Educational / Research',
  RX: 'Pharmacy',
  VET: 'Veterinary',
  IND: 'Industrial / Occupational'
};

// Size tiers - the suffix in segment codes like PO_large
const SIZE_LABELS = {
  new: 'New account',
  small: 'Small',
  mid: 'Mid-size',
  large: 'Large',
  enterprise: 'Enterprise'
};

// Specialty codes - standard McKesson SPCLTY_CD values
const SPECIALTY_LABELS = {
  FP: 'Family Practice',
  IM: 'Internal Medicine',
  PD: 'Pediatrics',
  GP: 'General Practice',
  EM: 'Emergency Medicine',
  GS: 'General Surgery',
  OBG: 'Obstetrics & Gynecology',
  ON: 'Oncology',
  HL: 'Hematology',
  R: 'Radiology',
  D: 'Dermatology',
  O: 'Ophthalmology',
  CHC: 'Community Health Center',
  HIA: 'Home Infusion / Ambulatory',
  SKL: 'Skilled Nursing',
  SC: 'Surgery Center',
  EDU: 'Educational / Academic',
  // Multispecialty groups
  M01: 'Multispecialty Group',
  M04: 'Multispecialty Group (Internal)',
  M07: 'Multispecialty Group (Surgical)',
  M08: 'Multispecialty Group (Diagnostic)',
  M14: 'Multispecialty Group (Womens Health)',
  M16: 'Multispecialty Group (Pediatric)',
  MUL: 'Multispecialty Group'
};

// Archetypes - matches the labels defined in compute_customer_archetypes.py
const ARCHETYPE_LABELS = {
  government: 'Government / Military',
  marketplace_reseller: 'Marketplace / Reseller',
  home_infusion: 'Home Infusion Pharmacy',
  home_care_provider: 'Home Care / Hospice',
  hospital_acute: 'Hospital / Acute Care',
  skilled_nursing: 'Skilled Nursing / Long-Term Care',
  surgery_center: 'Surgery Center',
  lab_pathology: 'Lab / Pathology',
  veterinary: 'Veterinary',
  educational: 'Educational / Research',
  pharmacy: 'Pharmacy',
  multispecialty_group: 'Multispecialty Group Practice',
  community_health: 'Community Health Center',
  pediatric: 'Pediatric Practice',
  primary_care: 'Primary Care',
  specialty_clinic: 'Specialty Clinic',
  unknown: 'Unclassified'
};

const SUPPLIER_PROFILE_LABELS = {
  mixed: 'Mixed (multiple suppliers)',
  dedicated: 'Dedicated (single supplier)',
  mckesson_heavy: 'McKesson-heavy',
  diversified: 'Diversified',
  national: 'National brands',
  private_brand: 'Private brands'
};

const LIFECYCLE_LABELS = {
  new: 'New',
  active: 'Active',
  stable: 'Stable',
  stable_warm: 'Stable',
  warm: 'Warm',
  declining: 'Declining',
  declining_warm: 'Declining',
  lapsing: 'Lapsing',
  lapsed: 'Lapsed',
  churned: 'Churned',
  reactivated: 'Reactivated'
};

// Public lookups - all return the original code as fallback so a missing
// translation doesn't blank out the field.

export function marketLabel(code) {
  if (!code) return '-';
  return MARKET_LABELS[code] || code;
}

export function sizeTierLabel(code) {
  if (!code) return '-';
  return SIZE_LABELS[code] || titleCase(code);
}

// segment is "MARKET_size" e.g. "PO_large" -> "Physician Office (Large)"
export function segmentLabel(code) {
  if (!code) return '-';
  const parts = code.split('_');
  if (parts.length === 1) return MARKET_LABELS[parts[0]] || code;
  const market = parts[0];
  const size = parts.slice(1).join('_');
  const marketName = MARKET_LABELS[market] || market;
  const sizeName = SIZE_LABELS[size] || titleCase(size);
  return `${marketName} (${sizeName})`;
}

export function specialtyLabel(code) {
  if (!code) return '-';
  return SPECIALTY_LABELS[code] || code;
}

export function archetypeLabel(code) {
  if (!code) return '-';
  return ARCHETYPE_LABELS[code] || titleCase((code || '').replace(/_/g, ' '));
}

export function supplierProfileLabel(code) {
  if (!code) return '-';
  return SUPPLIER_PROFILE_LABELS[code] || titleCase((code || '').replace(/_/g, ' '));
}

export function lifecycleLabel(code) {
  if (!code) return '-';
  return LIFECYCLE_LABELS[code] || titleCase((code || '').replace(/_/g, ' '));
}

function titleCase(s) {
  if (!s) return s;
  return s
    .split(' ')
    .map((w) => (w.length === 0 ? w : w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()))
    .join(' ');
}

// Option-list exports for use in <select> dropdowns. Each returns an
// array of { code, label } sorted alphabetically by label.

export function marketOptions() {
  return Object.keys(MARKET_LABELS)
    .map((code) => ({ code, label: MARKET_LABELS[code] }))
    .sort((a, b) => a.label.localeCompare(b.label));
}

export function sizeOptions() {
  // Preserve the natural new/small/mid/large/enterprise order rather than
  // alphabetic - that order reads as a size scale to the user.
  const order = ['new', 'small', 'mid', 'large', 'enterprise'];
  return order
    .filter((c) => SIZE_LABELS[c])
    .map((code) => ({ code, label: SIZE_LABELS[code] }));
}

export function specialtyOptions() {
  return Object.keys(SPECIALTY_LABELS)
    .map((code) => ({ code, label: SPECIALTY_LABELS[code] }))
    .sort((a, b) => a.label.localeCompare(b.label));
}
