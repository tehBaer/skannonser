// Popup DOM builder + inline kommentar/tag editor (Phase 5 Task 6).
//
// buildPopupContent(item, destinations) returns a DOM node for
// MapLibre's Popup.setDOMContent(). The node carries a self-contained
// annotation editor that PUTs /api/annotations/{finnkode} on save and
// mutates `item` (the shared per-listing object) in place so a re-open
// reflects the saved values.

const NOK = new Intl.NumberFormat("nb-NO");

function fmtPris(value) {
  if (value === null || value === undefined || value === "") return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return NOK.format(Math.round(n)) + " kr";
}

// Short destination label from its config key: brj -> BRJ, mvv -> MVV,
// mvv_uni -> UNI (segment after the last underscore, uppercased) -- yields
// the BRJ/MVV/UNI trio the map has always shown.
function shortDest(key) {
  const parts = String(key).split("_");
  return parts[parts.length - 1].toUpperCase();
}

function el(tag, cls, text) {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text !== undefined && text !== null) node.textContent = text;
  return node;
}

function addRow(dl, label, value) {
  if (value === null || value === undefined || value === "") return;
  dl.appendChild(el("dt", null, label));
  dl.appendChild(el("dd", null, String(value)));
}

// destinations: [{key,label}] from /api/meta (for the travel-minute rows).
export function buildPopupContent(item, destinations) {
  const root = el("div", "sk-popup");

  // Thumbnail (hidden on load error -- no broken-image icon).
  if (item.image) {
    const img = el("img", "thumb");
    img.src = "/thumbs/" + encodeURIComponent(item.finnkode) + ".jpg";
    img.alt = "";
    img.addEventListener("error", () => {
      img.style.display = "none";
    });
    root.appendChild(img);
  }

  const body = el("div", "body");

  const addr = el("p", "adresse", item.adresse || "(ukjent adresse)");
  const tag = el("span", "source-tag" + (item.sold ? " sold" : item.source === "dnb" ? " dnb" : ""));
  tag.textContent = item.sold ? "Solgt" : item.source === "dnb" ? "DNB" : "Eie";
  addr.appendChild(tag);
  body.appendChild(addr);

  const prisText = fmtPris(item.pris);
  if (prisText) {
    const pris = el("div");
    pris.appendChild(el("span", "pris", prisText));
    const kvm = fmtPris(item.pris_kvm);
    if (kvm) pris.appendChild(el("span", "kvm", kvm + "/m²"));
    body.appendChild(pris);
  }

  const dl = el("dl");
  const travel = item.travel || {};
  (destinations || []).forEach((d) => {
    const mins = travel[d.key];
    if (mins !== null && mins !== undefined && Number(mins) >= 0) {
      addRow(dl, shortDest(d.key), mins + " min");
    }
  });
  addRow(dl, "BRA-i", item.bra_i);
  addRow(dl, "Byggeår", item.byggeaar);
  addRow(dl, "Boligtype", item.boligtype);
  if (dl.childNodes.length) body.appendChild(dl);

  const links = el("div", "links");
  if (item.url) {
    const finn = el("a", null, "Finn");
    finn.href = item.url;
    finn.target = "_blank";
    finn.rel = "noopener";
    links.appendChild(finn);
  }
  if (item.lat != null && item.lng != null) {
    const gmap = el("a", null, "Google Maps");
    gmap.href = "https://www.google.com/maps?q=" + item.lat + "," + item.lng;
    gmap.target = "_blank";
    gmap.rel = "noopener";
    links.appendChild(gmap);
  }
  if (links.childNodes.length) body.appendChild(links);

  root.appendChild(body);
  root.appendChild(buildEditor(item));
  return root;
}

function buildEditor(item) {
  const editor = el("div", "sk-editor");

  editor.appendChild(el("label", null, "Kommentar"));
  const komInput = el("input");
  komInput.type = "text";
  komInput.value = item.kommentar || "";
  editor.appendChild(komInput);

  editor.appendChild(el("label", null, "Tag"));
  const tagInput = el("input");
  tagInput.type = "text";
  tagInput.value = item.tag || "";
  editor.appendChild(tagInput);

  const row = el("div", "row");
  const saveBtn = el("button", null, "Lagre");
  saveBtn.type = "button";
  const feedback = el("span");
  row.appendChild(saveBtn);
  row.appendChild(feedback);
  editor.appendChild(row);

  saveBtn.addEventListener("click", async () => {
    saveBtn.disabled = true;
    komInput.disabled = true;
    tagInput.disabled = true;
    feedback.className = "";
    feedback.textContent = "Lagrer …";
    try {
      const resp = await fetch(
        "/api/annotations/" + encodeURIComponent(item.finnkode),
        {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            kommentar: komInput.value.trim() || null,
            tag: tagInput.value.trim() || null,
          }),
        }
      );
      if (!resp.ok) throw new Error("HTTP " + resp.status);
      const saved = await resp.json();
      // Reflect the server's normalized values back into the shared item
      // so a re-open (and any table view) sees the saved state.
      item.kommentar = saved.kommentar;
      item.tag = saved.tag;
      komInput.value = saved.kommentar || "";
      tagInput.value = saved.tag || "";
      feedback.className = "saved";
      feedback.textContent = "Lagret ✓";
    } catch (err) {
      feedback.className = "error";
      feedback.textContent = "Feil: " + err.message;
    } finally {
      saveBtn.disabled = false;
      komInput.disabled = false;
      tagInput.disabled = false;
    }
  });

  return editor;
}
