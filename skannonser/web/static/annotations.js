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
