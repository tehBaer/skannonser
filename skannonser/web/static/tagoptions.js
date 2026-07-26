// One <datalist> of the tags that already exist, shared by every tag input on
// the page -- the map popup's editor and the table's Tag cells. Typing filters
// it; the arrow key (or the field's own dropdown affordance) shows the whole
// list. A native datalist rather than a hand-built autocomplete because it
// costs no focus/keyboard/positioning code and works inside the MapLibre popup,
// which is a floating element a custom dropdown would have to fight.
//
// It carries values only, no counts or tag colours: an <option label> renders
// INSTEAD of the value in some browsers, which would show "108" where the tag
// should be. Counts stay in the filter panel, where they already live.
import { normalizeTag } from "./tagcolors.js";

export const TAG_LIST_ID = "sk-tag-options";

// Normalized, deduped, sorted the same way assignTagColors sorts its keys, so
// the dropdown lists tags in the order the filter chips do.
export function tagOptionValues(tagKeys) {
  return [...new Set([...tagKeys].map(normalizeTag).filter(Boolean))]
    .sort((a, b) => a.localeCompare(b, "nb"));
}

// Idempotent: builds the datalist on first call, rewrites its options after.
// Call whenever the tag vocabulary can have changed -- saving an annotation can
// mint a brand new tag, and that tag must be suggestable on the next listing
// without a reload. Returns the values written, for callers that want to assert.
export function syncTagOptions(tagKeys, doc) {
  const d = doc || document;
  let list = d.getElementById(TAG_LIST_ID);
  if (!list) {
    list = d.createElement("datalist");
    list.id = TAG_LIST_ID;
    d.body.appendChild(list);
  }
  const values = tagOptionValues(tagKeys);
  list.textContent = "";
  values.forEach((value) => {
    const option = d.createElement("option");
    option.value = value;
    list.appendChild(option);
  });
  return values;
}

// Points one tag <input> at the shared list. Safe before syncTagOptions has
// run: `list` resolves by id at interaction time, so an input wired first and
// a datalist created later still find each other.
export function attachTagList(input) {
  input.setAttribute("list", TAG_LIST_ID);
  return input;
}
