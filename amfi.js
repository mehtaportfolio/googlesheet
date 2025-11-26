import { google } from "googleapis";
import dotenv from "dotenv";
import fetch from "node-fetch";
dotenv.config();

// ---- GOOGLE AUTH ----
const auth = new google.auth.JWT(
  process.env.GS_CLIENT_EMAIL,
  null,
  process.env.GS_PRIVATE_KEY.replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth });

// ---- Helpers ----
const norm = s => (s || "").toString().trim();
const numOrNull = v => {
  if (!v) return null;
  const n = Number(v.toString().replace(/,/g, "").trim());
  return isNaN(n) ? null : n;
};

async function readSheet(sheetId, range) {
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: sheetId, range });
  return res.data.values || [];
}

async function writeSheet(sheetId, range, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range,
    valueInputOption: "RAW",
    requestBody: { values },
  });
}

// ---- MAIN FUNCTION ----
export async function fastNAVUpdate(sheetName = process.env.GOOGLE_SHEET_NAME_MF) {
  console.log(`⏳ Running NAV Update for sheet: ${sheetName}`);

  const SHEET_ID = process.env.GOOGLE_SHEET_ID;

  // ---- 1️⃣ Read sheet ----
  const all = await readSheet(SHEET_ID, `${sheetName}!A1:Z10000`);
  if (!all.length || all.length < 2) {
    console.log("❌ No data rows found");
    return;
  }

  const headers = all[0];
  const rows = all.slice(1);

  const schemeCodeCol = headers.indexOf("Scheme Code");
  const dateCol = headers.indexOf("Date");
  const navCol = headers.indexOf("NAV");
  let statusCol = headers.indexOf("Status");

  if (schemeCodeCol === -1 || dateCol === -1 || navCol === -1) {
    console.log("❌ Missing required columns: Scheme Code / Date / NAV");
    return;
  }

  // Add 'Status' column if missing
  if (statusCol === -1) {
    statusCol = headers.length;
    headers.push("Status");
    rows.forEach(r => r.push(""));
  }

  // ---- 2️⃣ Fetch NAVAll.txt ----
  const url = "https://www.amfiindia.com/spages/NAVAll.txt";
  const response = await fetch(url, {
    headers: { "User-Agent": "Node.js" }
  });
  const content = await response.text();
  const lines = content.split(/\r?\n/);

  const navMap = new Map();
  lines.forEach(line => {
    const parts = line.split(";");
    if (parts.length >= 6) {
      const schemeCode = parts[0].trim();
      const nav = parts[parts.length - 2];
      const date = parts[parts.length - 1];
      if (schemeCode && nav && date) navMap.set(schemeCode, { nav, date });
    }
  });

  // ---- 3️⃣ Update sheet in memory ----
  let updated = 0, notFound = 0;
  rows.forEach(row => {
    const schemeCode = norm(row[schemeCodeCol]);
    if (!schemeCode) return;

    const found = navMap.get(schemeCode);
    if (found) {
      row[dateCol] = found.date;
      row[navCol] = found.nav;
      row[statusCol] = "✅ Updated";
      updated++;
    } else {
      row[statusCol] = "❌ Not Found";
      notFound++;
    }
  });

  // ---- 4️⃣ Write back ----
  const allUpdated = [headers, ...rows];
  await writeSheet(SHEET_ID, `${sheetName}!A1`, allUpdated);

  const summary = `✅ NAV Update Completed
Sheet: ${sheetName}
Updated: ${updated}
Not Found: ${notFound}
Total Rows: ${rows.length}`;

  console.log(summary);
  return {
    updated,
    notFound,
    total: rows.length,
    success: true
  };
}

// ---- Test locally ----
if (import.meta.url === `file://${process.argv[1]}`) {
  fastNAVUpdate();
}

