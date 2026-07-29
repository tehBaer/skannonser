// skannonser/web/static/tagpicker.js
// The map popup's tag control: one coloured chip per known tag, click to set,
// click the selected one again to clear, plus a field for minting a new tag.
//
// WHY not a dropdown. The tag input used to be a native <input list> pointed
// at tagoptions.js's shared <datalist>, which is still what the table's Tag
// cells use. A datalist costs no focus/keyboard/positioning code and survives
// inside a MapLibre popup -- but its options cannot be styled by any browser,
// so it can never show the tag colours. A custom dropdown PANEL is not an
// option either: .maplibregl-popup-content sets overflow:hidden (it clips the
// thumbnail into the popup's rounded corners) and would clip an absolutely
// positioned list. Anything that opens has to open in flow and grow the popup
// -- at which point it is this chip row with extra steps.
//
// This module builds DOM but holds no state of its own beyond the last painted
// values: the caller owns the tag and calls repaint() after it saves.
import { normalizeTag } from "./tagcolors.js";
import { tagOptionValues } from "./tagoptions.js";

// What the tag becomes when `clicked` is clicked while `current` is set.
// Pure, and the only place the click-again-clears rule lives.
export function nextTagValue(current, clicked) {
  const next = normalizeTag(clicked);
  return normalizeTag(current) === next ? "" : next;
}

export function buildTagPicker({ current, vocabulary, colorFor, onPick, doc }) {
  const d = doc || document;
  const node = d.createElement("div");
  node.className = "sk-tagpicker";

  const row = d.createElement("div");
  row.className = "tag-chip-row";
  node.appendChild(row);

  const newTag = d.createElement("input");
  newTag.type = "text";
  newTag.className = "sk-newtag";
  newTag.placeholder = "+ ny tag …";
  node.appendChild(newTag);

  let selected = normalizeTag(current);
  let chips = [];

  function paint(tags) {
    row.textContent = ""; // clear-the-children idiom, as in tagoptions.js
    chips = tagOptionValues(tags).map((tag) => {
      const chip = d.createElement("button");
      chip.type = "button";
      chip.textContent = tag;
      // Filled when selected, outlined otherwise -- the same reading the
      // filter panel's chips already have, so the colour never has to double
      // as the on/off signal.
      const on = tag === selected;
      chip.className = "tag-chip" + (on ? "" : " off");
      chip.setAttribute("aria-pressed", String(on));
      chip.style.setProperty("--tag-color", colorFor(tag) || "#6f7e76");
      chip.addEventListener("click", () => onPick(nextTagValue(selected, tag)));
      row.appendChild(chip);
      return chip;
    });
  }

  const commitNew = () => {
    const value = normalizeTag(newTag.value);
    if (!value) return; // whitespace is not a tag
    newTag.value = "";
    onPick(value);
  };

  newTag.addEventListener("blur", commitNew);
  newTag.addEventListener("keydown", (ev) => {
    if (ev.key === "Enter") {
      ev.preventDefault();
      commitNew();
    }
  });

  paint(vocabulary);

  return {
    node,
    repaint(currentTag, tags) {
      selected = normalizeTag(currentTag);
      paint(tags);
    },
    chipCount: () => chips.length,
    // The uncommitted contents of the new-tag field, for the caller's
    // flush-on-close path: the popup can be torn down before blur fires.
    pendingNewTag: () => normalizeTag(newTag.value),
  };
}
