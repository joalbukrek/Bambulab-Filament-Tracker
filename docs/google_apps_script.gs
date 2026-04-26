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
    writeTable(spreadsheet, SHEETS[key], data.headers, data.rows);
  }
  return jsonResponse({ ok: true, synced_at: new Date().toISOString() });
}

function writeTable(spreadsheet, sheetName, headers, rows) {
  const sheet = spreadsheet.getSheetByName(sheetName) || spreadsheet.insertSheet(sheetName);
  sheet.clearContents();
  const values = [headers].concat(rows || []);
  if (values.length === 0 || headers.length === 0) return;
  const colorColumn = headers.indexOf('Color') + 1;
  if (colorColumn > 0) {
    sheet.getRange(1, colorColumn, Math.max(values.length, 1), 1).setNumberFormat('@');
  }
  sheet.getRange(1, 1, values.length, headers.length).setValues(values);
  sheet.setFrozenRows(1);
  sheet.autoResizeColumns(1, headers.length);
}

function jsonResponse(body, statusCode) {
  return ContentService
    .createTextOutput(JSON.stringify(body))
    .setMimeType(ContentService.MimeType.JSON);
}
