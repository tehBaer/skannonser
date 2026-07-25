// skannonser/web/static/tagcolors.js
// Deterministic tag -> color assignment (2026-07-25 UI polish round 2, §1).
// A tag hashes (djb2) into a fixed palette; collisions probe to the next
// free slot with tags processed in SORTED order, so the assignment is a
// pure function of the current tag SET: distinct colors are guaranteed
// while <= palette-size tags exist, and a tag keeps its hash slot unless
// an earlier-sorted tag claims it. (Pure per-tag hashing was rejected:
// "maybe" and "definitivt" collide under djb2 mod 10.)

// Hue-offset from map.js's TYPE_COLOR_PALETTE (boligtype dots) so a tag
// ring is never confusable with its own dot color; all carry white text.
export const TAG_PALETTE = [
  "#c2185b", "#7b1fa2", "#303f9f", "#0277bd", "#00695c",
  "#558b2f", "#ff8f00", "#d84315", "#5d4037", "#455a64",
];

export function normalizeTag(tag) {
  return tag ? String(tag).trim().toLowerCase() : "";
}

function djb2(str) {
  let h = 5381;
  for (let i = 0; i < str.length; i++) h = ((h << 5) + h + str.charCodeAt(i)) >>> 0;
  return h;
}

// tagKeys: any iterable of raw tag values (dupes/empties fine).
export function assignTagColors(tagKeys) {
  const keys = [...new Set([...tagKeys].map(normalizeTag).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "nb"));
  const taken = new Set();
  const colors = new Map();
  keys.forEach((key) => {
    let idx = djb2(key) % TAG_PALETTE.length;
    // Probing only makes sense while free slots exist; past palette size,
    // collisions are unavoidable and plain hashing is the stable choice.
    if (keys.length <= TAG_PALETTE.length) {
      while (taken.has(idx)) idx = (idx + 1) % TAG_PALETTE.length;
    }
    taken.add(idx);
    colors.set(key, TAG_PALETTE[idx]);
  });
  return colors;
}

export function colorForTag(tag, colors) {
  return colors.get(normalizeTag(tag)) || null;
}
