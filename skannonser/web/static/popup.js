// Popup DOM builder + inline kommentar/tag editor.
//
// buildPopupContent(item, destinations, getTagColors) returns a DOM node for
// MapLibre's Popup.setDOMContent(). The node carries a self-contained
// annotation editor with no save button: the kommentar commits on blur/Enter,
// a tag commits the moment its chip is clicked, and app.js calls the node's
// skFlush() when the popup is torn down (see openPopup for why blur alone is
// not enough). Saves go through annotations.js's commitAnnotation -- the same
// helper table.js's inline cells use -- and mutate `item` (the shared
// per-listing object) in place so a re-open reflects the saved values.
//
// getTagColors is a FUNCTION, not a Map: app.js rebuilds state.tagColors on
// every recompute, and a tag invented in this editor has no colour until that
// rebuild has run.

import { commitAnnotation } from "./annotations.js";
import {
  isNew,
  fmtDate,
  premiumPct,
  fmtPremium,
  travelMinutes,
  mapsUrl,
  earthUrl,
  fmtJaNei,
  fmtFerdigattest,
  fmtUtleie,
  fmtHusdyr,
} from "./listingmeta.js";
import { colorForTag } from "./tagcolors.js";
import { buildTagPicker } from "./tagpicker.js";

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
// getTagColors: () => Map from tagcolors.js's assignTagColors, kept in sync
// with the table/map's palette so the popup chip matches the cell accent.
export function buildPopupContent(item, destinations, getTagColors) {
  const colors = () => (getTagColors && getTagColors()) || new Map();
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

  // Rebuilt rather than built once: the editor below can change the tag while
  // the popup is open, and a header chip still showing the old tag reads as a
  // bug. Declared here so buildEditor's save can call it.
  let miniChip = null;
  function refreshMiniChip() {
    if (miniChip) {
      miniChip.remove();
      miniChip = null;
    }
    const tagColor = colorForTag(item.tag, colors());
    if (!tagColor) return;
    miniChip = el("span", "tag-chip-mini", String(item.tag).trim());
    miniChip.style.background = tagColor;
    addr.appendChild(miniChip);
  }
  refreshMiniChip();
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

  // Sentinel commutes are dropped rather than shown as "-1 min"; this used to
  // be an inline `>= 0` check, now the same helper the table and the filter
  // use so the three views cannot drift apart.
  (destinations || []).forEach((d) => {
    const mins = travelMinutes(item, d.key);
    if (mins !== null) {
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

  // Salgsoppgave enrichment (migration 015). Every row is conditional -- a
  // listing whose prospectus never discussed a topic costs no row at all, so
  // this block is invisible on the ~1/3 of listings with no parsed text.
  addRow(dl, "Ferdigattest", fmtFerdigattest(item.ferdigattest));
  addRow(dl, "Eiendomsskatt", fmtPris(item.eiendomsskatt_kr));
  addRow(dl, "Verditakst", fmtPris(item.verditakst));
  addRow(dl, "Utleie", fmtUtleie(item.utleie));
  addRow(dl, "Husdyr", fmtHusdyr(item.husdyr));
  addRow(dl, "Heftelser", fmtJaNei(item.heftelser));
  addRow(dl, "Radon omtalt", fmtJaNei(item.radon_omtalt));
  addRow(dl, "Boligselgerforsikring", fmtJaNei(item.boligselgerforsikring));

  if (dl.childNodes.length) body.appendChild(dl);

  const links = el("div", "links");
  function addExternalLink(label, href) {
    if (!href) return; // un-geocoded listing: no link beats a link to nowhere
    const a = el("a", null, label);
    a.href = href;
    a.target = "_blank";
    a.rel = "noopener";
    links.appendChild(a);
  }
  addExternalLink("Finn", item.url);
  addExternalLink("Google Maps", mapsUrl(item));
  addExternalLink("Google Earth", earthUrl(item));
  const tbl = el("a", null, "Tabell");
  // Same-tab on purpose: Kart -> Tabell is in-app navigation, unlike the
  // external Finn/Maps links.
  tbl.href = "/table#finnkode=" + encodeURIComponent(item.finnkode);
  links.appendChild(tbl);
  if (links.childNodes.length) body.appendChild(links);

  root.appendChild(body);
  const nabolagSection = buildNabolagSection(item);
  if (nabolagSection) root.appendChild(nabolagSection);
  const editor = buildEditor(item, colors, refreshMiniChip);
  root.appendChild(editor);
  // app.js calls this on both teardown paths -- see openPopup.
  root.skFlush = editor.skFlush;
  return root;
}

// `colors` is () => Map; `onSaved` repaints the header chip.
function buildEditor(item, colors, onSaved) {
  const editor = el("div", "sk-editor");

  editor.appendChild(el("label", null, "Kommentar"));
  const komInput = el("input");
  komInput.type = "text";
  komInput.value = item.kommentar || "";
  editor.appendChild(komInput);

  editor.appendChild(el("label", null, "Tag"));
  const picker = buildTagPicker({
    current: item.tag,
    vocabulary: [...colors().keys()],
    colorFor: (tag) => colorForTag(tag, colors()),
    onPick: (value) => save({ tag: value }, null),
  });
  editor.appendChild(picker.node);

  // Saves are SERIALIZED, not guarded by an "is one in flight?" boolean.
  // Clicking a chip blurs the kommentar field first, so a chip click routinely
  // arrives while the kommentar's own PUT is still in flight -- and a guard
  // that drops the second call would silently lose the tag the user just
  // clicked. Chaining runs them in order instead.
  let chain = Promise.resolve();

  function save(patch, control) {
    chain = chain.then(() => runSave(patch, control));
    return chain;
  }

  // The kommentar ALWAYS travels with whatever the field currently shows.
  // Both controls are visible and auto-saving, so the visible state is the
  // intent; taking it off `item` instead would let a chip click overwrite
  // text the user had typed but not yet blurred. Only the tag comes from
  // `patch`, and only when the caller set one.
  //
  // `control` is the input to flash, or null when there is nothing to flash
  // (a chip click, or a flush on a popup that is already gone).
  async function runSave(patch, control) {
    komInput.classList.remove("saved", "error");
    picker.node.classList.remove("error");
    try {
      const saved = await commitAnnotation(item, {
        kommentar: komInput.value,
        tag: "tag" in patch ? patch.tag : item.tag,
      });
      if (!saved) return; // nothing changed; no PUT was sent
      komInput.value = saved.kommentar || "";
      if (control) {
        control.classList.add("saved");
        setTimeout(() => control.classList.remove("saved"), 1500);
      }
      // Order matters: app.js rebuilds state.tagColors inside this handler, so
      // a brand-new tag has no colour until it has run. Repainting first would
      // paint the new chip grey.
      document.dispatchEvent(
        new CustomEvent("sk-annotation-saved", { detail: { finnkode: item.finnkode } })
      );
      const before = picker.chipCount();
      picker.repaint(item.tag, [...colors().keys()]);
      onSaved();
      // A new chip can wrap the row onto another line; the popup grew, so ask
      // for the same re-pan the async nabolag section uses.
      if (picker.chipCount() !== before) {
        editor.dispatchEvent(new CustomEvent("sk-popup-resized", { bubbles: true }));
      }
    } catch (err) {
      // A chip click has no input to flash, but the popup is still on screen --
      // mark the picker itself. The flush path is the only case with genuinely
      // nowhere to show anything, because the popup is already gone.
      if (control) control.classList.add("error");
      else if (editor.isConnected) picker.node.classList.add("error");
      else console.warn("skannonser: lagring av notat feilet", err);
    }
  }

  komInput.addEventListener("blur", () => save({}, komInput));
  komInput.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      komInput.blur(); // triggers the blur listener above
    }
  });

  // Last chance to save before the DOM goes away. Browsers do not reliably
  // fire blur on a focused element that is removed from the document, so
  // neither listener above can be trusted to have run.
  editor.skFlush = () => {
    const pending = picker.pendingNewTag();
    return save(pending ? { tag: pending } : {}, null);
  };

  return editor;
}
