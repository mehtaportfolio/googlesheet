// -------------------------------
// IMPORTS (ES MODULE)
// -------------------------------
import { google } from "googleapis";
import dotenv from "dotenv";
dotenv.config();

// -------------------------------
// CONFIG
// -------------------------------
const SHEET_ID = "1gXrhmYx0IjIp-F4IZAf4mAqHJEbIAZER2U3MOLX2dZY";

// -------------------------------
// GOOGLE AUTH (ESM)
// -------------------------------
async function getSheetsClient() {
const serviceAccount = JSON.parse(
  Buffer.from(process.env.GS_JSON_BASE64, "base64").toString("utf-8")
);

const auth = new google.auth.GoogleAuth({
  credentials: serviceAccount,   // Use the decoded JSON
  scopes: ["https://www.googleapis.com/auth/spreadsheets"]
});


  return google.sheets({ version: "v4", auth: await auth.getClient() });
}

// -------------------------------
// SAFE DATE UTILITIES
// -------------------------------
function parseDate(val) {
  if (!val) return null;

  let d = new Date(val);
  if (!isNaN(d)) return d;

  // handle dd-mm-yyyy or dd/mm/yyyy
  if (/^\d{2}[-/]\d{2}[-/]\d{4}$/.test(val)) {
    const [dd, mm, yyyy] = val.split(/[-/]/);
    d = new Date(`${yyyy}-${mm}-${dd}`);
    if (!isNaN(d)) return d;
  }

  return null;
}

function formatDate(d) {
  if (!(d instanceof Date) || isNaN(d)) return "";
  return new Date(d.getTime() + 5.5 * 3600 * 1000)
    .toISOString()
    .slice(0, 10);
}

// -------------------------------
// MAIN WORKFLOW
// -------------------------------
async function runMFWorkflow(sheetName = "MF") {
  const sheets = await getSheetsClient();

  const mf = await readSheet(sheets, sheetName);
  const mf1 = await readSheet(sheets, "MF1");

  if (!mf) return "❌ MF sheet not found";
  if (!mf1) return "❌ MF1 sheet not found";

  // STEP 1 → BEFORE NAV FETCH
  await updateLCPfromMF(sheets, mf, mf1);

  // STEP 2 → FETCH NEW NAVs
  const navMsg = await fastNAVUpdate(sheets, sheetName);

  // STEP 3 → AFTER NAV FETCH
  await updateCMPfromMF(sheets, mf, mf1);

  return navMsg + "\nCMP/LCP sync completed.";
}

// -------------------------------
// READ SHEET
// -------------------------------
async function readSheet(sheets, sheetName) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: SHEET_ID,
      range: `${sheetName}!A:Z`
    });

    return {
      name: sheetName,
      data: res.data.values || []
    };

  } catch (err) {
    console.log("readSheet error for:", sheetName);
    console.log(err.response?.data || err.message);
    return null;
  }
}

// -------------------------------
// WRITE SHEET
// -------------------------------
async function writeSheet(sheets, range, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

// -------------------------------
// STEP 1 — UPDATE LCP
// -------------------------------
async function updateLCPfromMF(sheets, mf, mf1) {
  const mfData = mf.data;
  const mf1Data = mf1.data;

  const header = mfData[0];
  const dateCol = header.indexOf("Date");
  const navCol = header.indexOf("NAV");

  const mf1Map = {};
  for (let i = 1; i < mf1Data.length; i++) {
    const isin = mf1Data[i][1];
    if (isin) mf1Map[isin] = i;
  }

  const today = new Date();
  const dayBeforeYesterday = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 2);

  for (let i = 1; i < mfData.length; i++) {
    const isin = mfData[i][1];
    const dt = parseDate(mfData[i][dateCol]);
    if (!dt || dt > dayBeforeYesterday) continue;

    const nav = mfData[i][navCol];
    const row = mf1Map[isin];
    if (row >= 0 && mf1Data[row]) {
      mf1Data[row][4] = nav;   // LCP column
    }
  }

  await writeSheet(sheets, "MF1!A1", mf1Data);
}

// -------------------------------
// STEP 3 — UPDATE CMP
// -------------------------------
async function updateCMPfromMF(sheets, mf, mf1) {
  const mfData = mf.data;
  const mf1Data = mf1.data;

  const header = mfData[0];
  const dateCol = header.indexOf("Date");
  const navCol = header.indexOf("NAV");

  const mf1Map = {};
  for (let i = 1; i < mf1Data.length; i++) {
    const isin = mf1Data[i][1];
    if (isin) mf1Map[isin] = i;
  }

  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = formatDate(yesterday);

  for (let i = 1; i < mfData.length; i++) {
    const isin = mfData[i][1];
    const dt = formatDate(parseDate(mfData[i][dateCol]));
    if (!isin || !dt || dt !== yesterdayStr) continue;

    const nav = mfData[i][navCol];
    const row = mf1Map[isin];
    if (row >= 0 && mf1Data[row]) {
      mf1Data[row][3] = nav; // CMP column
    }
  }

  await writeSheet(sheets, "MF1!A1", mf1Data);
}

// -------------------------------
// FAST NAV UPDATE (AMFI)
// -------------------------------
async function fastNAVUpdate(sheets, sheetName) {
  const sheet = await readSheet(sheets, sheetName);
  if (!sheet) return "❌ Sheet not found";

  const data = sheet.data;
  const header = data[0];

  let schemeCodeCol = header.indexOf("Scheme Code");
  let dateCol = header.indexOf("Date");
  let navCol = header.indexOf("NAV");
  let statusCol = header.indexOf("Status");

  // Add Status column if missing
  if (statusCol === -1) {
    statusCol = header.length;
    data[0][statusCol] = "Status";

    // Write ONLY the new header column
    await sheets.spreadsheets.values.update({
      spreadsheetId: SHEET_ID,
      range: `${sheetName}!${colLetter(statusCol + 1)}1`,
      valueInputOption: "RAW",
      requestBody: { values: [["Status"]] }
    });
  }

  // Fetch AMFI NAV data
  const response = await fetch("https://www.amfiindia.com/spages/NAVAll.txt");
  const text = await response.text();

  const navMap = new Map();
  text.split(/\r?\n/).forEach(line => {
    const parts = line.split(";");
    if (parts.length >= 6) {
      const sc = parts[0].trim();
      navMap.set(sc, {
        nav: parts[parts.length - 2],
        date: parts[parts.length - 1]
      });
    }
  });

  let updated = 0, notFound = 0;
  const updates = []; // collect all update operations

  for (let i = 1; i < data.length; i++) {
    const code = String(data[i][schemeCodeCol]).trim();
    const row = i + 1; // Sheets rows start at 1
    const found = navMap.get(code);

    if (found) {
      updated++;

      updates.push({
        range: `${sheetName}!${colLetter(dateCol + 1)}${row}`,
        values: [[found.date]]
      });

      updates.push({
        range: `${sheetName}!${colLetter(navCol + 1)}${row}`,
        values: [[found.nav]]
      });

      updates.push({
        range: `${sheetName}!${colLetter(statusCol + 1)}${row}`,
        values: [["Updated"]]
      });

    } else {
      notFound++;
      updates.push({
        range: `${sheetName}!${colLetter(statusCol + 1)}${row}`,
        values: [["Not Found"]]
      });
    }
  }

  // Apply all updates in a single batch
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: SHEET_ID,
    requestBody: {
      valueInputOption: "RAW",
      data: updates
    }
  });

  return `NAV Update Completed  
Updated: ${updated}  
Not Found: ${notFound}`;
}

function colLetter(n) {
  let s = "";
  while (n > 0) {
    let mod = (n - 1) % 26;
    s = String.fromCharCode(65 + mod) + s;
    n = Math.floor((n - mod) / 26);
  }
  return s;
}

// -------------------------------
// DEBUG: LIST SHEETS
// -------------------------------
async function debugListSheets() {
  const sheets = await getSheetsClient();
  const meta = await sheets.spreadsheets.get({
    spreadsheetId: SHEET_ID
  });

  console.log("Available Sheets:");
  meta.data.sheets.forEach(s => console.log(" -", s.properties.title));
}

// -------------------------------
// RUN
// -------------------------------
debugListSheets()
  .then(() => {
    console.log("\nRunning workflow...\n");
    return runMFWorkflow("MF");
  })
  .then(console.log)
  .catch(console.error);
