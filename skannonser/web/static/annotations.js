// Shared annotation-save helper (Phase 5 Task 8): the single place that
// PUTs /api/annotations/{finnkode}. Extracted from popup.js's inline editor
// so table.js's inline kommentar/tag cells use the exact same save contract
// instead of duplicating it -- both callers get the server-normalized
// {finnkode, kommentar, tag} back and are responsible for reflecting it into
// their own shared item object / DOM.

export async function saveAnnotation(finnkode, kommentar, tag) {
  const resp = await fetch(
    "/api/annotations/" + encodeURIComponent(finnkode),
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        kommentar: (kommentar || "").trim() || null,
        tag: (tag || "").trim() || null,
      }),
    }
  );
  if (!resp.ok) throw new Error("HTTP " + resp.status);
  return resp.json(); // {finnkode, kommentar, tag} -- server-normalized
}

// Server-side normalization mirrored here (see saveAnnotation's payload above)
// so the dirty-check compares like with like: "", null and "  " are all the
// same (unset) value.
export function normalizeAnnotationValue(v) {
  return (v || "").trim() || null;
}

export function annotationChanged(item, kommentar, tag) {
  return (
    normalizeAnnotationValue(kommentar) !== normalizeAnnotationValue(item.kommentar) ||
    normalizeAnnotationValue(tag) !== normalizeAnnotationValue(item.tag)
  );
}

// The one way to save an annotation. Returns the server-normalized object, or
// null when the values matched what `item` already held and no PUT was sent.
//
// WHY the skip matters: every PUT bumps the row's updated_at even when the
// payload is byte-identical, and a bumped updated_at is exactly the signal
// sheet-import protection uses to treat an import-created row as "the user has
// edited this, don't overwrite it". A no-op blur was silently and permanently
// flipping that protection on for rows nobody actually touched. Both callers
// (table cells, popup editor) fire on blur, so both would hit it.
//
// `item` is mutated only on success: a failed PUT must leave it dirty so the
// next blur retries.
export async function commitAnnotation(item, { kommentar, tag }) {
  if (!annotationChanged(item, kommentar, tag)) return null;
  const saved = await saveAnnotation(item.finnkode, kommentar, tag);
  item.kommentar = saved.kommentar;
  item.tag = saved.tag;
  return saved;
}
