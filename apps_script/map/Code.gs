/**
 * Thin Apps Script adapter for interactive property map.
 *
 * Business logic should stay in the repo Python codebase.
 * This script only reads visible rows from Google Sheets and serves map data.
 */

const DEFAULT_LISTINGS_SHEET = 'Eie';
const DEFAULT_STATIONS_SHEET = 'Stations';
const DEFAULT_FINN_POLYGON_SHEET = 'Finn Polygon Coords';
const SEARCH_POLYGON_BUFFER_KM = 2;
const NORWAY_BOUNDS = {
  minLat: 57,
  maxLat: 72,
  minLng: 4,
  maxLng: 32,
};

function doGet(e) {
  if (e && e.parameter && e.parameter.health === '1') {
    return healthResponse_();
  }

  const template = HtmlService.createTemplateFromFile('map');
  template.bootstrap = JSON.stringify(getBootstrapData_());

  return template
    .evaluate()
    .setTitle('Property Map')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function healthResponse_() {
  try {
    const props = PropertiesService.getScriptProperties();
    const spreadsheetId = (props.getProperty('SPREADSHEET_ID') || '').trim();
    const listingsSheet = props.getProperty('LISTINGS_SHEET') || DEFAULT_LISTINGS_SHEET;

    const ss = resolveSpreadsheet_();
    const listingSheet = ss.getSheetByName(listingsSheet);
    const listingStatus = listingSheet ? 'ok' : `missing sheet: ${listingsSheet}`;

    const payload = {
      ok: true,
      timestamp: new Date().toISOString(),
      spreadsheetIdConfigured: spreadsheetId.length > 0,
      spreadsheetIdLength: spreadsheetId.length,
      spreadsheetTitle: ss.getName(),
      listingsSheetStatus: listingStatus,
    };

    return ContentService
      .createTextOutput(JSON.stringify(payload, null, 2))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (error) {
    const payload = {
      ok: false,
      timestamp: new Date().toISOString(),
      error: String(error),
    };

    return ContentService
      .createTextOutput(JSON.stringify(payload, null, 2))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

function getBootstrapData_() {
  const props = PropertiesService.getScriptProperties();
  return {
    mapApiKey: props.getProperty('MAPS_API_KEY') || '',
    defaultListingsSheet: props.getProperty('LISTINGS_SHEET') || DEFAULT_LISTINGS_SHEET,
    defaultStationsSheet: props.getProperty('STATIONS_SHEET') || DEFAULT_STATIONS_SHEET,
  };
}

/**
 * Fetch listings from currently visible (not filtered out) rows and station overlays.
 */
function getMapData(listingsSheetName, stationsSheetName, options) {
  const t0 = Date.now();
  const ss = resolveSpreadsheet_();
  const tAfterSpreadsheet = Date.now();

  const listingSheet = ss.getSheetByName(listingsSheetName || DEFAULT_LISTINGS_SHEET);
  const stationSheet = ss.getSheetByName(stationsSheetName || DEFAULT_STATIONS_SHEET);

  if (!listingSheet) {
    throw new Error('Listings sheet not found: ' + (listingsSheetName || DEFAULT_LISTINGS_SHEET));
  }

  const respectSheetFilters = Boolean(options && options.respectSheetFilters === true);
  const searchPolygon = getFinnSearchPolygon_();
  const searchBounds = getSearchBoundsFromPolygon_(searchPolygon, SEARCH_POLYGON_BUFFER_KM);
  const listingResult = getVisibleListings_(listingSheet, respectSheetFilters, searchBounds, searchPolygon);
  const tAfterListings = Date.now();

  const stations = stationSheet ? getStations_(stationSheet) : [];
  const tAfterStations = Date.now();

  const timingsMs = {
    total: tAfterStations - t0,
    openSpreadsheet: tAfterSpreadsheet - t0,
    readListings: tAfterListings - tAfterSpreadsheet,
    readStations: tAfterStations - tAfterListings,
  };

  return {
    generatedAt: new Date().toISOString(),
    listings: listingResult.rows,
    stations: stations,
    diagnostics: {
      spreadsheetName: ss.getName(),
      listingsSheet: listingSheet.getName(),
      stationsSheet: stationSheet ? stationSheet.getName() : '',
      totalDataRows: listingResult.meta.totalDataRows,
      scannedRows: listingResult.meta.scannedRows,
      scanLimited: listingResult.meta.scanLimited,
      visibleRows: listingResult.meta.visibleRows,
      hiddenByFilter: listingResult.meta.hiddenByFilter,
      hiddenByUser: listingResult.meta.hiddenByUser,
      strictVisibilityChecksApplied: listingResult.meta.strictVisibilityChecksApplied,
      skippedEmptyRows: listingResult.meta.skippedEmptyRows,
      missingLatLngVisibleRows: listingResult.meta.missingLatLngVisibleRows,
      excludedOutsideNorway: listingResult.meta.excludedOutsideNorway,
      outsideNorwaySamples: listingResult.meta.outsideNorwaySamples,
      outsideNorwayRows: listingResult.meta.outsideNorwayRows,
      searchBounds: listingResult.meta.searchBounds,
      searchPolygon: searchPolygon,
      timingsMs: timingsMs,
    },
  };
}

function getFinnSearchPolygon_() {
  // Sheet-only behavior: always read polygon points from dedicated sheet.
  const props = PropertiesService.getScriptProperties();
  const polygonSheetName = (props.getProperty('FINN_POLYGON_SHEET') || DEFAULT_FINN_POLYGON_SHEET).trim();
  const ss = resolveSpreadsheet_();
  const polygonSheet = ss.getSheetByName(polygonSheetName);
  if (!polygonSheet) {
    return [];
  }

  return readPolygonFromSheet_(polygonSheet);
}

function readPolygonFromSheet_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  if (lastRow < 2 || lastCol < 2) {
    return [];
  }

  const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(normalizeHeader_);
  let latIdx = headers.indexOf('LAT');
  let lngIdx = headers.indexOf('LNG');

  // Allow legacy order-based layouts: A=Order, B=LAT, C=LNG.
  if (latIdx < 0 && lastCol >= 2) latIdx = 1;
  if (lngIdx < 0 && lastCol >= 3) lngIdx = 2;
  if (latIdx < 0 || lngIdx < 0) {
    return [];
  }

  const values = sheet.getRange(2, 1, lastRow - 1, lastCol).getValues();
  const points = [];
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const lat = toNumberOrNull_(row[latIdx]);
    const lng = toNumberOrNull_(row[lngIdx]);
    if (lat == null || lng == null) {
      continue;
    }
    points.push({ lat: lat, lng: lng });
  }

  return points;
}

function resolveSpreadsheet_() {
  const props = PropertiesService.getScriptProperties();
  const spreadsheetId = (props.getProperty('SPREADSHEET_ID') || '').trim();

  if (spreadsheetId) {
    return SpreadsheetApp.openById(spreadsheetId);
  }

  const active = SpreadsheetApp.getActiveSpreadsheet();
  if (active) {
    return active;
  }

  throw new Error('Spreadsheet not configured. Set Script Property SPREADSHEET_ID to your Google Sheet ID.');
}

function getVisibleListings_(sheet, respectSheetFilters, searchBounds, searchPolygon) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();
  const maxRowsToScan = 5000;

  if (lastRow < 2 || lastCol < 1) {
    return {
      rows: [],
      meta: {
        totalDataRows: Math.max(0, lastRow - 1),
        scannedRows: 0,
        scanLimited: false,
        visibleRows: 0,
        hiddenByFilter: 0,
        hiddenByUser: 0,
        strictVisibilityChecksApplied: false,
        skippedEmptyRows: 0,
        missingLatLngVisibleRows: 0,
        excludedOutsideNorway: 0,
        outsideNorwaySamples: [],
        outsideNorwayRows: [],
        searchBounds: searchBounds,
      },
    };
  }

  const headers = sheet.getRange(1, 1, 1, lastCol).getValues()[0].map(normalizeHeader_);
  const totalDataRows = lastRow - 1;
  const rowsToScan = Math.min(totalDataRows, maxRowsToScan);
  const values = sheet.getRange(2, 1, rowsToScan, lastCol).getValues();

  const latIdx = headers.indexOf('LAT');
  const lngIdx = headers.indexOf('LNG');
  const finnkodeIdx = headers.indexOf('Finnkode');
  const adresseIdx = headers.indexOf('ADRESSE') >= 0 ? headers.indexOf('ADRESSE') : headers.indexOf('Adresse');
  const hasActiveFilter = sheet.getFilter() != null;

  const out = [];
  // Fast by default. Respect filtered-out rows only when explicitly requested.
  const strictVisibilityChecksApplied = Boolean(respectSheetFilters && hasActiveFilter);
  let hiddenByFilter = 0;
  let hiddenByUser = 0;
  let skippedEmptyRows = 0;
  let missingLatLngVisibleRows = 0;
  let excludedOutsideNorway = 0;
  const outsideNorwaySamples = [];
  const outsideNorwayRows = [];

  for (let i = 0; i < values.length; i++) {
    const rowNumber = i + 2;
    const row = values[i];

    // Skip clearly empty rows before hidden-row checks (which are expensive).
    const hasFinnkode = finnkodeIdx >= 0 && String(row[finnkodeIdx] || '').trim() !== '';
    const hasAdresse = adresseIdx >= 0 && String(row[adresseIdx] || '').trim() !== '';
    if (!hasFinnkode && !hasAdresse) {
      skippedEmptyRows += 1;
      continue;
    }

    if (strictVisibilityChecksApplied && sheet.isRowHiddenByFilter(rowNumber)) {
      hiddenByFilter += 1;
      continue;
    }

    const obj = {};
    for (let c = 0; c < headers.length; c++) {
      obj[headers[c]] = row[c];
    }

    const finnkode = normalizeFinnkode_(obj.Finnkode);
    const finnUrl = parseUrl_(obj.URL);

    const latValue = latIdx >= 0 ? toNumberOrNull_(row[latIdx]) : null;
    const lngValue = lngIdx >= 0 ? toNumberOrNull_(row[lngIdx]) : null;
    if (latValue == null || lngValue == null) {
      missingLatLngVisibleRows += 1;
    } else if (!isWithinSearchArea_(latValue, lngValue, searchPolygon, searchBounds)) {
      excludedOutsideNorway += 1;
      outsideNorwayRows.push({
        Finnkode: finnkode,
        ADRESSE: valueOrEmpty_(obj.ADRESSE || obj.Adresse),
        URL: finnUrl,
        LAT: latValue,
        LNG: lngValue,
      });
      if (outsideNorwaySamples.length < 5) {
        outsideNorwaySamples.push({
          Finnkode: finnkode,
          ADRESSE: valueOrEmpty_(obj.ADRESSE || obj.Adresse),
          LAT: latValue,
          LNG: lngValue,
        });
      }
      continue;
    }

    out.push({
      rowNumber: rowNumber,
      Finnkode: finnkode,
      ADRESSE: valueOrEmpty_(obj.ADRESSE || obj.Adresse),
      Postnummer: valueOrEmpty_(obj.Postnummer),
      Pris: valueOrEmpty_(obj.Pris),
      Tilgjengelighet: valueOrEmpty_(obj.Tilgjengelighet),
      GOOGLE_MAPS_URL: parseUrl_(obj.GOOGLE_MAPS_URL),
      URL: finnUrl,
      IMAGE_HOSTED_URL: parseUrl_(obj.IMAGE_HOSTED_URL),
      IMAGE_URL: parseUrl_(obj.IMAGE_URL),
      LAT: latValue,
      LNG: lngValue,
      details: obj,
    });
  }

  return {
    rows: out,
    meta: {
      totalDataRows: totalDataRows,
      scannedRows: rowsToScan,
      scanLimited: totalDataRows > rowsToScan,
      visibleRows: out.length,
      hiddenByFilter: hiddenByFilter,
      hiddenByUser: hiddenByUser,
      strictVisibilityChecksApplied: strictVisibilityChecksApplied,
      skippedEmptyRows: skippedEmptyRows,
      missingLatLngVisibleRows: missingLatLngVisibleRows,
      excludedOutsideNorway: excludedOutsideNorway,
      outsideNorwaySamples: outsideNorwaySamples,
      outsideNorwayRows: outsideNorwayRows,
      searchBounds: searchBounds,
    },
  };
}

function isWithinBounds_(lat, lng, bounds) {
  const target = bounds || NORWAY_BOUNDS;
  return (
    lat >= target.minLat &&
    lat <= target.maxLat &&
    lng >= target.minLng &&
    lng <= target.maxLng
  );
}

function getSearchBoundsFromPolygon_(searchPolygon, bufferKm) {
  if (!searchPolygon || searchPolygon.length < 3) {
    return NORWAY_BOUNDS;
  }

  let minLat = Infinity;
  let maxLat = -Infinity;
  let minLng = Infinity;
  let maxLng = -Infinity;

  for (let i = 0; i < searchPolygon.length; i++) {
    const point = searchPolygon[i] || {};
    const lat = Number(point.lat);
    const lng = Number(point.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      continue;
    }
    minLat = Math.min(minLat, lat);
    maxLat = Math.max(maxLat, lat);
    minLng = Math.min(minLng, lng);
    maxLng = Math.max(maxLng, lng);
  }

  if (!Number.isFinite(minLat) || !Number.isFinite(maxLat) || !Number.isFinite(minLng) || !Number.isFinite(maxLng)) {
    return NORWAY_BOUNDS;
  }

  const effectiveBufferKm = Number(bufferKm);
  if (Number.isFinite(effectiveBufferKm) && effectiveBufferKm > 0) {
    const avgLat = (minLat + maxLat) / 2;
    const latBufferDeg = effectiveBufferKm / 111.32;
    const cosLat = Math.max(0.1, Math.cos(avgLat * Math.PI / 180));
    const lngBufferDeg = effectiveBufferKm / (111.32 * cosLat);
    minLat -= latBufferDeg;
    maxLat += latBufferDeg;
    minLng -= lngBufferDeg;
    maxLng += lngBufferDeg;
  }

  return {
    minLat: minLat,
    maxLat: maxLat,
    minLng: minLng,
    maxLng: maxLng,
  };
}

function isWithinSearchArea_(lat, lng, searchPolygon, searchBounds) {
  if (!isWithinBounds_(lat, lng, searchBounds)) {
    return false;
  }

  if (!searchPolygon || searchPolygon.length < 3) {
    // When no FINN polygon is configured, keep the coarse bounds fallback.
    return true;
  }

  if (isPointInPolygon_(lat, lng, searchPolygon)) {
    return true;
  }

  return isPointWithinPolygonBuffer_(lat, lng, searchPolygon, SEARCH_POLYGON_BUFFER_KM);
}

function isPointInPolygon_(lat, lng, polygon) {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const pi = polygon[i] || {};
    const pj = polygon[j] || {};
    const yi = Number(pi.lat);
    const xi = Number(pi.lng);
    const yj = Number(pj.lat);
    const xj = Number(pj.lng);

    if (!Number.isFinite(yi) || !Number.isFinite(xi) || !Number.isFinite(yj) || !Number.isFinite(xj)) {
      continue;
    }

    const intersects = ((yi > lat) !== (yj > lat)) &&
      (lng < ((xj - xi) * (lat - yi)) / ((yj - yi) || 1e-12) + xi);

    if (intersects) {
      inside = !inside;
    }
  }

  return inside;
}

function isPointWithinPolygonBuffer_(lat, lng, polygon, bufferKm) {
  const effectiveBufferKm = Number(bufferKm);
  if (!Number.isFinite(effectiveBufferKm) || effectiveBufferKm <= 0) {
    return false;
  }

  const bufferMeters = effectiveBufferKm * 1000;
  const metersPerDegLat = 111320;
  const metersPerDegLng = 111320 * Math.max(0.1, Math.cos(lat * Math.PI / 180));

  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const pi = polygon[i] || {};
    const pj = polygon[j] || {};
    const yi = Number(pi.lat);
    const xi = Number(pi.lng);
    const yj = Number(pj.lat);
    const xj = Number(pj.lng);

    if (!Number.isFinite(yi) || !Number.isFinite(xi) || !Number.isFinite(yj) || !Number.isFinite(xj)) {
      continue;
    }

    const ax = (xj - lng) * metersPerDegLng;
    const ay = (yj - lat) * metersPerDegLat;
    const bx = (xi - lng) * metersPerDegLng;
    const by = (yi - lat) * metersPerDegLat;

    if (distancePointToSegmentMeters_(0, 0, ax, ay, bx, by) <= bufferMeters) {
      return true;
    }
  }

  return false;
}

function distancePointToSegmentMeters_(px, py, ax, ay, bx, by) {
  const abx = bx - ax;
  const aby = by - ay;
  const apx = px - ax;
  const apy = py - ay;
  const abLenSq = abx * abx + aby * aby;

  if (abLenSq <= 0) {
    const dx = px - ax;
    const dy = py - ay;
    return Math.sqrt(dx * dx + dy * dy);
  }

  let t = (apx * abx + apy * aby) / abLenSq;
  t = Math.max(0, Math.min(1, t));

  const cx = ax + t * abx;
  const cy = ay + t * aby;
  const dx = px - cx;
  const dy = py - cy;
  return Math.sqrt(dx * dx + dy * dy);
}

function getStations_(sheet) {
  const lastRow = sheet.getLastRow();
  const lastCol = sheet.getLastColumn();

  if (lastRow < 1 || lastCol < 1) {
    return [];
  }

  const firstRow = sheet.getRange(1, 1, 1, lastCol).getValues()[0];
  const normalizedFirstRow = firstRow.map(normalizeHeader_);

  const hasHeaderRow =
    normalizedFirstRow.indexOf('Name') >= 0 &&
    normalizedFirstRow.indexOf('LAT') >= 0 &&
    normalizedFirstRow.indexOf('LNG') >= 0;

  // Support both layouts:
  // 1) Header row in row 1 (Name, LAT, LNG, optional RadiusM, optional Type)
  // 2) Legacy no-header rows with 3-5 columns.
  let headers = normalizedFirstRow;
  if (!hasHeaderRow) {
    if (lastCol >= 5) {
      headers = ['Name', 'LAT', 'LNG', 'RadiusM', 'Type'];
    } else if (lastCol === 4) {
      // In 4-column legacy sheets, col4 can be either RadiusM (numeric) or Type (text).
      const col4Numeric = toNumberOrNull_(firstRow[3]);
      headers = col4Numeric == null
        ? ['Name', 'LAT', 'LNG', 'Type']
        : ['Name', 'LAT', 'LNG', 'RadiusM'];
    } else {
      headers = ['Name', 'LAT', 'LNG'];
    }
  }

  const startRow = hasHeaderRow ? 2 : 1;
  const valueRows = lastRow - startRow + 1;
  if (valueRows <= 0) {
    return [];
  }

  const values = sheet.getRange(startRow, 1, valueRows, lastCol).getValues();

  const idxName = headers.indexOf('Name');
  const idxLat = headers.indexOf('LAT');
  const idxLng = headers.indexOf('LNG');
  const idxRadius = headers.indexOf('RadiusM');
  const idxType = headers.indexOf('Type');

  const out = [];
  for (let i = 0; i < values.length; i++) {
    const row = values[i];
    const lat = idxLat >= 0 ? toNumberOrNull_(row[idxLat]) : null;
    const lng = idxLng >= 0 ? toNumberOrNull_(row[idxLng]) : null;
    if (lat == null || lng == null) {
      continue;
    }

    const radiusValue = idxRadius >= 0 ? toNumberOrNull_(row[idxRadius]) : null;

    out.push({
      Name: idxName >= 0 ? valueOrEmpty_(row[idxName]) : 'Station',
      LAT: lat,
      LNG: lng,
      RadiusM: radiusValue != null && radiusValue > 0 ? radiusValue : null,
      Type: idxType >= 0 ? valueOrEmpty_(row[idxType]) : 'train',
    });
  }

  return out;
}

function normalizeHeader_(value) {
  return String(value || '').trim();
}

function normalizeFinnkode_(value) {
  const str = String(value || '').trim();
  if (!str) {
    return '';
  }

  // Handle HYPERLINK formulas where display text is the finnkode.
  if (/^=HYPERLINK\(/i.test(str)) {
    const parts = str.split('"');
    if (parts.length >= 4) {
      return String(parts[3]).trim();
    }
  }

  return str.replace(/\.0$/, '');
}

function valueOrEmpty_(value) {
  return value == null ? '' : value;
}

function toNumberOrNull_(value) {
  if (value === '' || value == null) {
    return null;
  }
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function parseUrl_(value) {
  const str = String(value || '').trim();
  if (!str) {
    return '';
  }

  if (/^=HYPERLINK\(/i.test(str)) {
    const parts = str.split('"');
    if (parts.length >= 2) {
      return parts[1] || '';
    }
  }

  return str;
}

/**
 * Resolve thumbnail URLs for selected listings by reading og:image from FINN pages.
 * Input: [{ key: string, url: string }]
 * Output: { [key]: thumbnailUrl }
 */
function getListingThumbnails(requests) {
  const out = {};
  const items = Array.isArray(requests) ? requests : [];
  if (!items.length) {
    return out;
  }

  const cache = CacheService.getScriptCache();
  const maxItems = Math.min(items.length, 20);

  for (let i = 0; i < maxItems; i++) {
    const req = items[i] || {};
    const key = String(req.key || '').trim();
    const url = parseUrl_(req.url);

    if (!key) {
      continue;
    }

    if (!url) {
      out[key] = '';
      continue;
    }

    const cacheKey = 'thumb:' + Utilities.base64EncodeWebSafe(url).slice(0, 220);
    const cached = cache.get(cacheKey);
    if (cached != null) {
      out[key] = cached;
      continue;
    }

    let thumbnailUrl = '';
    let thumbnailValue = '';
    let errorReason = '';
    try {
      let html = '';
      let status = 0;
      const firstRes = fetchFinnHtml_(url);
      status = firstRes.status;
      html = firstRes.html;

      if (!(status >= 200 && status < 300)) {
        errorReason = 'HTTP ' + status;
      }

      thumbnailUrl = extractOgImageFromHtml_(html);
      if (!thumbnailUrl) {
        thumbnailUrl = extractFinnImageUrlFromHtml_(html);
      }

      if (!thumbnailUrl) {
        const fk = extractFinnkodeFromUrl_(url);
        if (fk) {
          const canonicalUrl = 'https://www.finn.no/realestate/homes/ad.html?finnkode=' + fk;
          const secondRes = fetchFinnHtml_(canonicalUrl);
          if (secondRes.status >= 200 && secondRes.status < 300) {
            thumbnailUrl = extractOgImageFromHtml_(secondRes.html) || extractFinnImageUrlFromHtml_(secondRes.html);
          }
          if (!thumbnailUrl && !errorReason && secondRes.status) {
            errorReason = 'No image metadata (HTTP ' + secondRes.status + ')';
          }
        } else if (!errorReason) {
          errorReason = 'No image metadata';
        }
      }
    } catch (err) {
      thumbnailUrl = '';
      errorReason = summarizeThumbnailError_(err);
    }

    if (thumbnailUrl) {
      // Inline data URLs avoid browser-side cross-origin image blocking in Apps Script iframes.
      thumbnailValue = toInlineImageDataUrl_(thumbnailUrl) || thumbnailUrl;
    }

    if (!thumbnailValue && errorReason) {
      thumbnailValue = '__ERR__:' + errorReason;
    }

    out[key] = thumbnailValue;
    if (thumbnailValue) {
      putSmallCacheValue_(cache, cacheKey, thumbnailValue, 60 * 60 * 6);
    } else {
      putSmallCacheValue_(cache, cacheKey, '', 60 * 30);
    }
  }

  return out;
}

function fetchFinnHtml_(url) {
  const res = UrlFetchApp.fetch(url, {
    muteHttpExceptions: true,
    followRedirects: true,
    headers: {
      'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
      'Accept-Language': 'nb-NO,nb;q=0.9,en-US;q=0.8,en;q=0.7',
      'Referer': 'https://www.finn.no/',
      'Upgrade-Insecure-Requests': '1',
    },
  });

  return {
    status: Number(res.getResponseCode()),
    html: res.getContentText(),
  };
}

function extractFinnkodeFromUrl_(url) {
  const str = String(url || '').trim();
  if (!str) return '';
  const match = str.match(/[?&]finnkode=(\d+)/i);
  return match && match[1] ? match[1] : '';
}

function summarizeThumbnailError_(err) {
  const msg = String(err || 'Unknown error');
  if (/permission|authorization|required|scope/i.test(msg)) {
    return 'UrlFetch authorization required';
  }
  if (/timed?\s*out|deadline/i.test(msg)) {
    return 'Request timeout';
  }
  return msg.slice(0, 140);
}

function putSmallCacheValue_(cache, key, value, ttlSeconds) {
  const str = String(value || '');
  // CacheService rejects large payloads; skip caching oversized values.
  if (str.length > 90000) {
    return;
  }
  cache.put(key, str, ttlSeconds);
}

function toInlineImageDataUrl_(imageUrl) {
  const url = String(imageUrl || '').trim();
  if (!url) {
    return '';
  }

  try {
    const imgRes = UrlFetchApp.fetch(url, {
      muteHttpExceptions: true,
      followRedirects: true,
      headers: {
        'User-Agent': 'Mozilla/5.0 (compatible; PropertyMapBot/1.0)',
        'Accept': 'image/avif,image/webp,image/apng,image/*,*/*;q=0.8',
      },
    });

    const status = Number(imgRes.getResponseCode());
    if (!(status >= 200 && status < 300)) {
      return '';
    }

    const headers = imgRes.getHeaders();
    const contentType = String(headers['Content-Type'] || headers['content-type'] || '').toLowerCase();
    if (!contentType.startsWith('image/')) {
      return '';
    }

    const bytes = imgRes.getBlob().getBytes();
    if (!bytes || !bytes.length) {
      return '';
    }

    // Keep payload size bounded for HTML/script response performance.
    if (bytes.length > 350000) {
      return '';
    }

    const b64 = Utilities.base64Encode(bytes);
    return 'data:' + contentType + ';base64,' + b64;
  } catch (_err) {
    return '';
  }
}

function extractOgImageFromHtml_(html) {
  const text = String(html || '');
  if (!text) {
    return '';
  }

  const patterns = [
    /<meta[^>]+(?:property|name)=["']og:image["'][^>]+content=["']([^"']+)["'][^>]*>/i,
    /<meta[^>]+content=["']([^"']+)["'][^>]+(?:property|name)=["']og:image["'][^>]*>/i,
  ];

  for (let i = 0; i < patterns.length; i++) {
    const match = text.match(patterns[i]);
    if (match && match[1]) {
      const raw = decodeHtmlEntities_(String(match[1]).trim());
      return normalizeImageUrl_(raw);
    }
  }

  return '';
}

function extractFinnImageUrlFromHtml_(html) {
  const text = String(html || '');
  if (!text) {
    return '';
  }

  const match = text.match(/https:\/\/images\.finncdn\.no[^"'\s>]+/i);
  if (!match || !match[0]) {
    return '';
  }

  return normalizeImageUrl_(decodeHtmlEntities_(match[0]));
}

function normalizeImageUrl_(url) {
  const str = String(url || '').trim();
  if (!str) {
    return '';
  }

  const normalized = str
    .replace(/\\\//g, '/')
    .replace(/\\u0026/gi, '&')
    .replace(/&amp;/g, '&');

  if (/^https?:\/\//i.test(normalized)) {
    return normalized;
  }

  return '';
}

function decodeHtmlEntities_(text) {
  return String(text || '')
    .replace(/&amp;/g, '&')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}
