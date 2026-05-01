// Variant stem

// Group SKUs that are size/dose variants of the same product family.
// "Gauze Pad 4x4 inch Sterile" and "Gauze Pad 2x2 inch Sterile" should
// collapse to one card. The user picks per-size quantities in a modal.

// Tokens we strip from descriptions to get the family stem. Order matters:
// we strip composite size patterns first, then doses, then pack counts,
// then trailing isolated numbers. Anything left is the stem.
const STRIP_PATTERNS = [
  // 4x4 in, 2 x 2 inch, 8x10cm, 4-ply etc
  /\b\d+\s*(?:x|×|by)\s*\d+\s*(?:inch|inches|in|cm|mm|ft|"|'|ply)?\b/gi,
  // 4 inch, 8 in, 5 ft
  /\b\d+(?:\.\d+)?\s*(?:inch|inches|in|cm|mm|ft|m|"|')\b/gi,
  // dose: 5 mg, 200 mcg, 10 ml, 250 cc, 10 g, 1 l, 2 fl oz, 100 IU, 200 units
  /\b\d+(?:\.\d+)?\s*(?:mg|mcg|μg|ug|g|kg|ml|l|cc|fl\s*oz|oz|iu|units?)\b/gi,
  // 100/Box, 50/case, 10/pk
  /\b\d+\s*\/\s*(?:box|bx|case|cs|pack|pk|tray|tr|sleeve|bag|carton|ctn|each|ea)\b/gi,
  // Box of 50, Pack of 10, Case of 24
  /\b(?:box|case|pack|tray|sleeve|bag|carton)\s+of\s+\d+\b/gi,
  // 50 ct, 100 count, 24 pk
  /\b\d+\s*(?:ct|count|pk|pack|each|ea)\b/gi,
  // Trailing number+unit like "12 pads", "30 wipes" (very common)
  /\b\d+\s*(?:pads?|wipes?|sheets?|tabs?|tablets?|capsules?|tubes?|rolls?|sponges?|swabs?|gloves?|masks?|gowns?)\b/gi,
  // Size letters at word boundary: XS, S, M, L, XL, XXL when alone
  /\b(?:xxl|xl|xs|sm|sml|md|lg|small|medium|large|x-?large|x-?small)\b/gi,
  // Size word: "Size 7", "Size 12"
  /\bsize\s*\d+\b/gi,
  // Latex/non-latex variants? (NO - those are not size variants, leave them)
  // Trailing standalone numbers (after the above strips, often left over)
  /\s+\d+\s*$/g,
  // Comma followed by stripped fragments, double spaces
  /\s+,\s+/g
];

// Tokens that look like a size/variant LABEL (what we keep to identify
// the variant inside a group). Roughly the inverse of STRIP_PATTERNS but
// matching the matched text rather than removing it.
const VARIANT_LABEL_PATTERNS = [
  /\b\d+\s*(?:x|×|by)\s*\d+\s*(?:inch|inches|in|cm|mm|ft|"|'|ply)?\b/i,
  /\b\d+(?:\.\d+)?\s*(?:inch|inches|in|cm|mm|ft|m|"|')\b/i,
  /\b\d+(?:\.\d+)?\s*(?:mg|mcg|μg|ug|g|kg|ml|l|cc|fl\s*oz|oz|iu|units?)\b/i,
  /\b\d+\s*\/\s*(?:box|bx|case|cs|pack|pk|tray|tr|sleeve|bag|carton|ctn|each|ea)\b/i,
  /\b(?:box|case|pack|tray|sleeve|bag|carton)\s+of\s+\d+\b/i,
  /\b\d+\s*(?:ct|count|pk|pack|each|ea)\b/i,
  /\b\d+\s*(?:pads?|wipes?|sheets?|tabs?|tablets?|capsules?|tubes?|rolls?|sponges?|swabs?|gloves?|masks?|gowns?)\b/i,
  /\b(?:xxl|xl|xs|sm|sml|md|lg|small|medium|large|x-?large|x-?small)\b/i,
  /\bsize\s*\d+\b/i
];

// Compute the stem (grouping key) of a product description.
// Returns lowercased, whitespace-collapsed string with size/dose tokens stripped.
export function variantStem(description) {
  if (!description || typeof description !== 'string') return '';
  let s = description;
  for (let i = 0; i < STRIP_PATTERNS.length; i = i + 1) {
    s = s.replace(STRIP_PATTERNS[i], ' ');
  }
  // Collapse punctuation/whitespace runs
  s = s.replace(/[\s,]+/g, ' ').trim();
  // Strip dangling separators
  s = s.replace(/^[\s,\-|/]+|[\s,\-|/]+$/g, '');
  return s.toLowerCase();
}

// Pull out the most variant-distinguishing token to use as the variant
// label inside the modal (e.g. "4x4 in", "2x2 in"). If nothing matches,
// fall back to the full description so each variant still has a label.
export function variantLabel(description) {
  if (!description || typeof description !== 'string') return '';
  const labels = [];
  for (let i = 0; i < VARIANT_LABEL_PATTERNS.length; i = i + 1) {
    const m = description.match(VARIANT_LABEL_PATTERNS[i]);
    if (m) labels.push(m[0].trim());
  }
  if (labels.length === 0) return description;
  // De-dupe but preserve order
  const seen = new Set();
  const uniq = [];
  for (let i = 0; i < labels.length; i = i + 1) {
    const k = labels[i].toLowerCase();
    if (!seen.has(k)) {
      seen.add(k);
      uniq.push(labels[i]);
    }
  }
  return uniq.join(' • ');
}

// Build a stable group key combining stem with family + private-brand flag.
// We do NOT group across families even if the stem matches, since two
// products with the same name in different families are genuinely different.
export function groupKey(item) {
  if (!item) return '';
  const stem = variantStem(item.description || '');
  const fam = (item.family || '').toLowerCase();
  const cat = (item.category || '').toLowerCase();
  const pb = item.is_private_brand ? '1' : '0';
  // If the stem is too short (1-2 chars), fall back to per-item-id grouping
  // to avoid collapsing unrelated items.
  if (stem.length < 4) return `solo:${item.item_id}`;
  return `${fam}|${cat}|${pb}|${stem}`;
}

// Group an array of items by groupKey. Returns an array of group objects
// with a representative item (the one with highest stock or first) and the
// full variant list. Preserves the original ordering by using the first
// occurrence of each group key.
export function groupItems(items) {
  if (!Array.isArray(items)) return [];
  const groups = new Map();
  const order = [];
  for (let i = 0; i < items.length; i = i + 1) {
    const it = items[i];
    const key = groupKey(it);
    if (!groups.has(key)) {
      groups.set(key, []);
      order.push(key);
    }
    groups.get(key).push(it);
  }
  const result = [];
  for (let i = 0; i < order.length; i = i + 1) {
    const key = order[i];
    const variants = groups.get(key);
    // Pick representative: prefer in-stock, then highest stock, then first
    const rep = pickRepresentative(variants);
    result.push({
      key,
      representative: rep,
      variants,
      variantCount: variants.length
    });
  }
  return result;
}

function pickRepresentative(variants) {
  if (variants.length === 1) return variants[0];
  let best = variants[0];
  let bestScore = -1;
  for (let i = 0; i < variants.length; i = i + 1) {
    const v = variants[i];
    const stock = typeof v.units_in_stock === 'number' ? v.units_in_stock : 0;
    // Prefer in-stock, then by stock count, then by has-image (we don't, so skip)
    const score = (stock > 0 ? 100000 : 0) + stock;
    if (score > bestScore) {
      bestScore = score;
      best = v;
    }
  }
  return best;
}

// Suppress variants of cart-helper suggestions that are already represented
// in the current cart. Given a list of suggestions (each with item_id and a
// description) and a Set of group keys already in the cart, return only
// suggestions whose group key is NOT in the cart's group set.
export function filterSuggestionsAgainstCart(suggestions, cartGroupKeys) {
  if (!Array.isArray(suggestions)) return [];
  if (!cartGroupKeys || cartGroupKeys.size === 0) return suggestions;
  return suggestions.filter((s) => {
    const key = groupKey({
      item_id: s.item_id || s.pb_item_id || s.mckesson_item_id,
      description: s.description || s.pb_description || s.mckesson_description,
      family: s.family,
      category: s.category,
      is_private_brand: s.is_mckesson_brand
    });
    return !cartGroupKeys.has(key);
  });
}

// Build a Set of group keys for the items currently in cart. Used by the
// suggestion filter above.
export function cartGroupKeySet(cartItems) {
  const set = new Set();
  if (!Array.isArray(cartItems)) return set;
  for (let i = 0; i < cartItems.length; i = i + 1) {
    set.add(groupKey(cartItems[i]));
  }
  return set;
}
