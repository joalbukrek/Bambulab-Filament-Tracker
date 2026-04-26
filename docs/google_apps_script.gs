const SECRET = 'CHANGE_ME_TO_A_RANDOM_SECRET';

const SHEETS = {
  ams_slots: 'AMS Slots',
  spools: 'Spools',
  print_jobs: 'Print Jobs',
  usage: 'Usage',
};

function doPost(e) {
  const payload = JSON.parse(e.postData.contents);
  if (payload.secret !== SECRET) {
    return jsonResponse({ ok: false, error: 'invalid secret' }, 403);
  }

  const spreadsheet = SpreadsheetApp.getActiveSpreadsheet();
  for (const key of Object.keys(SHEETS)) {
    const data = payload.tables[key];
    if (!data) continue;
    appendUniqueTable(spreadsheet, SHEETS[key], data.headers, data.rows);
  }
  return jsonResponse({ ok: true, synced_at: new Date().toISOString() });
}

function appendUniqueTable(spreadsheet, sheetName, headers, rows) {
  const sheet = spreadsheet.getSheetByName(sheetName) || spreadsheet.insertSheet(sheetName);
  if (!headers || headers.length === 0) return;

  const existingRows = sheet.getLastRow();
  const existingColumns = Math.max(sheet.getLastColumn(), headers.length);
  if (existingRows === 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    sheet.setFrozenRows(1);
  }

  const colorColumn = headers.indexOf('Color') + 1;
  if (colorColumn > 0) {
    sheet.getRange(1, colorColumn, Math.max(sheet.getMaxRows(), 1), 1).setNumberFormat('@');
  }

  const existing = new Set();
  const currentLastRow = sheet.getLastRow();
  if (currentLastRow > 1) {
    const values = sheet.getRange(2, 1, currentLastRow - 1, existingColumns).getValues();
    for (const row of values) {
      existing.add(rowSignature(row.slice(0, headers.length)));
    }
  }

  const rowsToAppend = [];
  for (const row of rows || []) {
    const normalized = normalizeRow(row, headers.length);
    const signature = rowSignature(normalized);
    if (existing.has(signature)) continue;
    existing.add(signature);
    rowsToAppend.push(normalized);
  }

  if (rowsToAppend.length > 0) {
    sheet.getRange(sheet.getLastRow() + 1, 1, rowsToAppend.length, headers.length).setValues(rowsToAppend);
  }
  sheet.autoResizeColumns(1, headers.length);
}

function normalizeRow(row, width) {
  const normalized = (row || []).slice(0, width);
  while (normalized.length < width) normalized.push('');
  return normalized;
}

function rowSignature(row) {
  return JSON.stringify(row.map(value => String(value == null ? '' : value)));
}

function jsonResponse(body, statusCode) {
  return ContentService
    .createTextOutput(JSON.stringify(body))
    .setMimeType(ContentService.MimeType.JSON);
}
