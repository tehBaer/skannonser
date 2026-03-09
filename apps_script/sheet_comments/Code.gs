/**
 * Apps Script to record editor and timestamp when Kommentar or Tag columns are edited.
 *
 * Installable trigger: use Edit > Current project's triggers > Add trigger
 * - Choose function: onEditTrigger
 * - Event: From spreadsheet / On edit
 *
 * It writes timestamp into columns named 'Kommentar__edited_at', 'Kommentar__edited_by',
 * and similar for 'Tag'. Ensure those header names exist (they can be hidden columns).
 */

function onEditTrigger(e) {
  try {
    var sh = e.range.getSheet();
    if (!sh) return;
    if (sh.getName() !== 'Eie') return; // adjust sheet name if needed
    var row = e.range.getRow();
    if (row === 1) return; // ignore header edits

    var col = e.range.getColumn();
    var headerRange = sh.getRange(1, 1, 1, sh.getLastColumn());
    var headers = headerRange.getValues()[0];
    var hdr = (headers[col - 1] || '').toString().trim();

    var user = '';
    try {
      user = Session.getActiveUser ? Session.getActiveUser().getEmail() : '';
    } catch (err) {
      user = '';
    }

    var now = new Date();

    if (hdr === 'Kommentar') {
      var atCol = findHeaderColumn_(sh, 'Kommentar__edited_at');
      var byCol = findHeaderColumn_(sh, 'Kommentar__edited_by');
      if (atCol) sh.getRange(row, atCol).setValue(now);
      if (byCol && user) sh.getRange(row, byCol).setValue(user);
    } else if (hdr === 'Tag') {
      var atCol = findHeaderColumn_(sh, 'Tag__edited_at');
      var byCol = findHeaderColumn_(sh, 'Tag__edited_by');
      if (atCol) sh.getRange(row, atCol).setValue(now);
      if (byCol && user) sh.getRange(row, byCol).setValue(user);
    }
  } catch (err) {
    Logger.log('onEditTrigger error: ' + err);
  }
}

function findHeaderColumn_(sheet, name) {
  var headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  for (var i = 0; i < headers.length; i++) {
    if ((headers[i] || '').toString().trim() === name) return i + 1;
  }
  return null;
}
