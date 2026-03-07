/**
 * Thin Apps Script adapter for interactive property map.
 *
 * Business logic should stay in the repo Python codebase.
 * This script only reads visible rows from Google Sheets and serves map data.
 */

const DEFAULT_LISTINGS_SHEET = 'Eie';
const DEFAULT_STATIONS_SHEET = 'Stations';
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
  const listingResult = getVisibleListings_(listingSheet, respectSheetFilters);
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
      timingsMs: timingsMs,
    },
  };
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

function getVisibleListings_(sheet, respectSheetFilters) {
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
    } else if (!isInNorwayBounds_(latValue, lngValue)) {
      excludedOutsideNorway += 1;
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
    },
  };
}

function isInNorwayBounds_(lat, lng) {
  return (
    lat >= NORWAY_BOUNDS.minLat &&
    lat <= NORWAY_BOUNDS.maxLat &&
    lng >= NORWAY_BOUNDS.minLng &&
    lng <= NORWAY_BOUNDS.maxLng
  );
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
