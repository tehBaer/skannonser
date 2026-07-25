// Popup DOM builder + inline kommentar/tag editor (Phase 5 Task 6).
//
// buildPopupContent(item, destinations, tagColors) returns a DOM node for
// MapLibre's Popup.setDOMContent(). The node carries a self-contained
// annotation editor that PUTs /api/annotations/{finnkode} on save (via the
// shared ./annotations.js helper -- table.js's inline cells use the same
// one) and mutates `item` (the shared per-listing object) in place so a
// re-open reflects the saved values.

import { saveAnnotation } from "./annotations.js";
import { isNew, fmtDate, premiumPct, fmtPremium } from "./listingmeta.js";
import { colorForTag } from "./tagcolors.js";

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
  if (value instanceof Node) {
    const dd = el("dd");
    dd.appendChild(value);
    dl.appendChild(dd);
  } else {
    dl.appendChild(el("dd", null, String(value)));
  }
}


// "Solgt i nabolaget": sales the sold-price sweep discovered in this
// listing's ~120 m query boxes -- incl. sales we never tracked. Lazy: the
// fetch fires when the popup is built and fills in when it lands. Data
// accumulates from sweep responses only (no backfill exists), so this reads
// "ingen … ennå" for most listings at first.
//
// The header + empty-state placeholder are built and appended SYNCHRONOUSLY
// (before the fetch even starts), not on resolve. openPopup calls
// setDOMContent then panPopupIntoView(), which measures and pans within a
// single requestAnimationFrame -- well before the network fetch can
// resolve. If the section started as a bare empty div and only grew once the
// fetch landed, the pan would measure a shorter popup than the one the user
// ends up with, pushing the annotation editor below the viewport (the exact
// failure panPopupIntoView exists to prevent -- see its comment in app.js).
// Pre-rendering the empty-state line gives the pan the right height for the
// common case (no backfill means most listings have no neighbours yet); on
// resolve we either swap in the real rows or leave the placeholder as-is.
function buildNabolagSection(item) {
  if (item.source === "dnb") return null; // DNB ids never anchor sweep boxes
  if (!item.closed) return null; // only closed (Solgt/Inaktiv/Trukket) listings
  // can ever be a sweep target (select_sold_targets), so an active listing's
  // discovered_near_finnkode is empty by construction, forever -- skip the
  // request and empty-state DOM entirely rather than promise a fill that
  // never comes.
  const wrap = el("div", "sk-nabolag");
  const head = el("p", "sk-nabolag-head", "Solgt i nabolaget");
  const empty = el("p", "muted sk-nabolag-empty", "ingen registrerte nabolagssalg ennå");
  wrap.appendChild(head);
  wrap.appendChild(empty);
  fetch("/api/listings/" + encodeURIComponent(item.finnkode) + "/nabolag")
    .then((resp) => (resp.ok ? resp.json() : { sales: [] }))
    .then(({ sales }) => {
      if (!sales.length) return; // placeholder already reads correctly
      head.textContent = "Solgt i nabolaget (" + sales.length + ")";
      wrap.removeChild(empty);
      const shown = sales.slice(0, 5);
      shown.forEach((s) => {
        const row = el("div", "sk-nabolag-row");
        if (s.tracked) {
          const a = el("a", null, s.address || s.finnkode);
          a.href = "/#finnkode=" + encodeURIComponent(s.finnkode);
          row.appendChild(a);
        } else {
          row.appendChild(el("span", null, s.address || "(ukjent adresse)"));
        }
        const parts = [];
        if (s.sold_price) parts.push(NOK.format(s.sold_price) + " kr");
        if (s.price_per_m2) parts.push(NOK.format(s.price_per_m2) + "/m²");
        const date = fmtDate(s.sold_date);
        if (date) parts.push(date);
        row.appendChild(el("span", "muted", parts.join(" · ")));
        wrap.appendChild(row);
      });
      // The API caps at 15; we only render 5 -- say so when there's more,
      // since the header count (sales.length) is neither.
      if (sales.length > shown.length) {
        wrap.appendChild(
          el("p", "muted sk-nabolag-more", "viser " + shown.length + " av " + sales.length)
        );
      }
      // The pre-rendered empty state covers the common case, but a listing
      // that DOES have neighbours grows by up to five rows after the pan has
      // already measured. Tell the map to re-pan; app.js owns the how.
      wrap.dispatchEvent(new CustomEvent("sk-popup-resized", { bubbles: true }));
    })
    .catch(() => {
      /* popup stays useful without the section; no error noise */
    });
  return wrap;
}

// destinations: [{key,label}] from /api/meta (for the travel-minute rows).
// tagColors: Map from tagcolors.js's assignTagColors, kept in sync with the
// table/map's palette so the popup chip matches the ring/cell accent.
export function buildPopupContent(item, destinations, tagColors) {
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
  const tagClass =
    "source-tag" +
    (item.sold ? " sold" : item.closed ? " inactive" : item.source === "dnb" ? " dnb" : "");
  const tag = el("span", tagClass);
  tag.textContent = item.sold
    ? "Solgt"
    : item.closed
      ? item.tilgjengelighet // derived "Inaktiv" / "Trukket"
      : item.source === "dnb"
        ? "DNB"
        : "Finn";
  addr.appendChild(tag);
  if (isNew(item)) addr.appendChild(el("span", "ny-badge", "Ny"));

  const tagColor = colorForTag(item.tag, tagColors || new Map());
  if (tagColor) {
    const chip = el("span", "tag-chip-mini", String(item.tag).trim());
    chip.style.background = tagColor;
    addr.appendChild(chip);
  }
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

  // Sold outcome (tinglyst) first, so the sale result sits right under the
  // last-seen asking price it should be read against.
  if (item.sold) {
    const soldText = fmtPris(item.sold_price);
    if (soldText) {
      addRow(dl, "Solgt for", soldText);
      const dateText = fmtDate(item.sold_date);
      if (dateText) addRow(dl, "Solgt dato", dateText);
      const pct = premiumPct(item);
      if (pct != null) {
        const span = el(
          "span",
          pct >= 0 ? "premie-pos" : "premie-neg",
          fmtPremium(pct) + " vs prisant."
        );
        addRow(dl, "Budpremie", span);
      }
    } else {
      addRow(dl, "Solgt pris", el("span", "ingen-solgtpris", "ingen tinglyst pris ennå"));
    }
  }

  const travel = item.travel || {};
  (destinations || []).forEach((d) => {
    const mins = travel[d.key];
    if (mins !== null && mins !== undefined && Number(mins) >= 0) {
      addRow(dl, shortDest(d.key), mins + " min");
    }
  });

  // Listing-details enrichment: the true cost picture + key filters.
  addRow(dl, "Totalpris", fmtPris(item.totalpris));
  addRow(dl, "Felleskost", fmtPris(item.felleskost_mnd) && fmtPris(item.felleskost_mnd) + "/mnd");
  addRow(dl, "Mnd-kost", fmtPris(item.maanedskost) && fmtPris(item.maanedskost) + "/mnd");

  addRow(dl, "BRA-i", item.bra_i);
  addRow(dl, "Byggeår", item.byggeaar);
  addRow(dl, "Boligtype", item.boligtype);
  addRow(dl, "Eieform", item.eieform);
  addRow(dl, "Soverom", item.soverom);
  if (item.energimerke) {
    addRow(
      dl,
      "Energi",
      item.energimerke + (item.energifarge ? " (" + item.energifarge + ")" : "")
    );
  }
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
  const nabolagSection = buildNabolagSection(item);
  if (nabolagSection) root.appendChild(nabolagSection);
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
      const saved = await saveAnnotation(item.finnkode, komInput.value, tagInput.value);
      // Reflect the server's normalized values back into the shared item
      // so a re-open (and any table view) sees the saved state.
      item.kommentar = saved.kommentar;
      item.tag = saved.tag;
      komInput.value = saved.kommentar || "";
      tagInput.value = saved.tag || "";
      feedback.className = "saved";
      feedback.textContent = "Lagret ✓";
      // Let app.js refresh tag-dependent UI (tag filter list, tag rings).
      document.dispatchEvent(
        new CustomEvent("sk-annotation-saved", { detail: { finnkode: item.finnkode } })
      );
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
