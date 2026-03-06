// -------------------------------
// IMPORTS (ES MODULE)
// -------------------------------
import { google } from "googleapis";
import fetch from "node-fetch";
import dotenv from "dotenv";
dotenv.config();

// -------------------------------
// CONFIG
// -------------------------------
const SHEET_ID = process.env.GOOGLE_SHEET_ID;

// -------------------------------
// GOOGLE AUTH (ESM)
// -------------------------------
async function getSheetsClient() {
  const serviceAccount = JSON.parse(
    Buffer.from(process.env.GS_JSON_BASE64, "base64").toString("utf-8")
  );

  const auth = new google.auth.JWT(
    serviceAccount.client_email,
    null,
    serviceAccount.private_key,
    ["https://www.googleapis.com/auth/spreadsheets"]
  );

  return google.sheets({ version: "v4", auth });
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

  // STEP 0 → Read both sheets in one go
  const res = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: SHEET_ID,
    ranges: [`${sheetName}!A:Z`, "MF1!A:Z"],
  });

  const valueRanges = res.data.valueRanges || [];
  const mfData = valueRanges[0]?.values || [];
  const mf1Data = valueRanges[1]?.values || [];

  const mf = { name: sheetName, data: mfData };
  const mf1 = { name: "MF1", data: mf1Data };

  if (mfData.length === 0) return "❌ MF sheet not found or empty";
  if (mf1Data.length === 0) return "❌ MF1 sheet not found or empty";

  // STEP 1 → BEFORE NAV FETCH
  await updateLCPfromMF(sheets, mf, mf1);

  // STEP 2 → FETCH NEW NAVs
  const navMsg = await fastNAVUpdate(sheets, sheetName, mfData);

  // RE-READ MF sheet to get updated NAVs for Step 3
  // (We could theoretically avoid this by updating mfData in memory during Step 2,
  // but let's keep it safe with one re-read for now or optimize further if needed)
  const mfUpdatedRes = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${sheetName}!A:Z`,
  });
  const mfUpdatedData = mfUpdatedRes.data.values || [];
  const mfUpdated = { name: sheetName, data: mfUpdatedData };

  // STEP 3 → AFTER NAV FETCH
  await updateCMPfromMF(sheets, mfUpdated, mf1);

  return navMsg + "\nCMP/LCP sync completed.";
}

// -------------------------------
// READ SHEET (KEEP FOR INDIVIDUAL CALLS IF NEEDED)
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
// STEP 1 — UPDATE LCP
// -------------------------------
async function updateLCPfromMF(sheets, mf, mf1) {
  const mfData = mf.data;
  const mf1Data = mf1.data;

  const header = mfData[0];
  const dateCol = header.indexOf("Date");
  const navCol = header.indexOf("NAV");
  const isinCol = header.indexOf("ISIN");

  const mf1Map = {};
  for (let i = 1; i < mf1Data.length; i++) {
    const isin = (mf1Data[i][1] || "").trim().toUpperCase();
    if (isin) mf1Map[isin] = i;
  }

  const today = new Date();
  const dayBeforeYesterday = new Date(today.getFullYear(), today.getMonth(), today.getDate() - 2);

  const updates = [];

  for (let i = 1; i < mfData.length; i++) {
    const isin = (mfData[i][isinCol] || "").trim().toUpperCase();
    const dt = parseDate(mfData[i][dateCol]);
    if (!dt || dt > dayBeforeYesterday) continue;

    const nav = mfData[i][navCol];
    const rowIdx = mf1Map[isin];
    if (rowIdx >= 0) {
      updates.push({
        range: `MF1!${colLetter(5)}${rowIdx + 1}`,   // LCP is Column E (5)
        values: [[nav]]
      });
    }
  }

  if (updates.length > 0) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        valueInputOption: "RAW",
        data: updates
      }
    });
  }
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
  const isinCol = header.indexOf("ISIN");

  const mf1Map = {};
  for (let i = 1; i < mf1Data.length; i++) {
    const isin = (mf1Data[i][1] || "").trim().toUpperCase();
    if (isin) mf1Map[isin] = i;
  }

  let maxDate = null;
  for (let i = 1; i < mfData.length; i++) {
    const dt = parseDate(mfData[i][dateCol]);
    if (dt && (!maxDate || dt > maxDate)) maxDate = dt;
  }
  
  const latestDateStr = formatDate(maxDate);
  console.log(`Syncing CMP for date: ${latestDateStr}`);

  const updates = [];

  for (let i = 1; i < mfData.length; i++) {
    const isin = (mfData[i][isinCol] || "").trim().toUpperCase();
    const dt = formatDate(parseDate(mfData[i][dateCol]));
    if (!isin || !dt || dt !== latestDateStr) continue;

    const nav = mfData[i][navCol];
    const rowIdx = mf1Map[isin];
    if (rowIdx >= 0) {
      updates.push({
        range: `MF1!${colLetter(4)}${rowIdx + 1}`, // CMP is Column D (4)
        values: [[nav]]
      });
    }
  }

  if (updates.length > 0) {
    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: SHEET_ID,
      requestBody: {
        valueInputOption: "RAW",
        data: updates
      }
    });
  }
}

// -------------------------------
// FAST NAV UPDATE (AMFI)
// -------------------------------
async function fastNAVUpdate(sheets, sheetName, initialData = null) {
  let data;
  if (initialData) {
    data = initialData;
  } else {
    const sheet = await readSheet(sheets, sheetName);
    if (!sheet) return "❌ Sheet not found";
    data = sheet.data;
  }

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
/*
debugListSheets()
  .then(() => {
    console.log("\nRunning workflow...\n");
    return runMFWorkflow("MF");
  })
  .then(console.log)
  .catch(console.error);
*/

export { fastNAVUpdate, runMFWorkflow };

