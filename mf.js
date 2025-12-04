import { google } from "googleapis";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

// ---------------------------
// GOOGLE AUTH (SERVICE ACCOUNT FILE)
// ---------------------------
// Decode Base64 JSON into object
const serviceAccount = JSON.parse(
  Buffer.from(process.env.GS_JSON_BASE64, "base64").toString("utf-8")
);

const auth = new google.auth.GoogleAuth({
  credentials: serviceAccount,       // <-- Use credentials directly
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});


const sheets = google.sheets({
  version: "v4",
  auth: await auth.getClient(),
});

// ---------------------------
// SUPABASE CLIENT
// ---------------------------
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);

// ---------------------------
// HELPERS
// ---------------------------
const norm = (v) => (v || "").toString().trim().toUpperCase();
const numOrNull = (v) => {
  if (v === "" || v == null) return null;
  const n = Number(v.toString().replace(/,/g, ""));
  return isNaN(n) ? null : n;
};

async function readSheet(sheetId, range) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range,
  });
  return res.data.values || [];
}

async function appendRows(sheetId, sheetName, rows) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: sheetId,
    range: `${sheetName}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: rows },
  });
}

// ---------------------------
// MAIN SYNC FUNCTION
// ---------------------------
export async function syncMF() {
  console.log("🔄 Running MF Sync…");

  const SHEET_ID = process.env.GOOGLE_SHEET_ID;
  const SHEET_NAME = process.env.GOOGLE_SHEET_NAME_MF;
  const TABLE = process.env.SUPABASE_TABLE_MF;

  // STEP 1 – SUPABASE
  console.log("📡 Fetching Supabase data...");
  const { data: supRows, error } = await supabase
    .from(TABLE)
    .select("isin,scheme_code,fund_full_name,cmp,lcp");

  if (error) throw new Error(error.message);

  const supMap = {};
  supRows.forEach((r) => (supMap[norm(r.isin)] = r));

  // STEP 2 – GOOGLE SHEET
  console.log("📄 Reading Google Sheet...");
  const all = await readSheet(SHEET_ID, `${SHEET_NAME}!A1:Z50000`);
  const headers = all[0];
  const rows = all.slice(1);

  const col = {};
  headers.forEach((h, i) => {
    col[norm(h)] = i;
  });

  const isinCol = col["ISIN"];
  const schemeCol = col["SCHEME CODE"];
  const cmpCol = col["CMP"];
  const lcpCol = col["LCP"];
  const schemeNameCol = col["SCHEME NAME"];

  const sheetISINs = {};
  const batchUpdates = [];

  // STEP 3 – Build batch update list
  rows.forEach((r) => {
    const isin = norm(r[isinCol]);
    const schemeCode = norm(r[schemeCol]);
    const cmp = numOrNull(r[cmpCol]);
    const lcp = numOrNull(r[lcpCol]);

    if (isin) sheetISINs[isin] = true;
    if (!isin && !schemeCode) return;

    batchUpdates.push({
      isin: isin || null,
      scheme_code: schemeCode || null,
      cmp,
      lcp,
    });
  });

  // STEP 4 – Bulk update via RPC
  console.log("📤 Sending RPC bulk update to Supabase…");

  if (batchUpdates.length > 0) {
    const rpcRes = await fetch(`${process.env.SUPABASE_URL}/rest/v1/rpc/bulk_update_funds`, {
      method: "POST",
      headers: {
        apikey: process.env.SUPABASE_API_KEY,
        Authorization: `Bearer ${process.env.SUPABASE_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ rows: batchUpdates }),
    });

    console.log("RPC Response:", await rpcRes.text());
  }

  // STEP 5 – Missing ISINs
  console.log("🔍 Detecting missing ISINs…");

  const missing = [];

  supRows.forEach((r) => {
    const isin = norm(r.isin);
    if (!isin) return;
    if (!sheetISINs[isin]) missing.push(r);
  });

  // STEP 6 – Insert missing ISIN rows into Sheet
  if (missing.length > 0) {
    console.log("➕ Adding", missing.length, "missing ISINs to sheet.");

    const newRows = missing.map((r) => {
      const row = new Array(headers.length).fill("");

      row[isinCol] = r.isin;
      row[schemeCol] = r.scheme_code;
      row[schemeNameCol] = r.fund_full_name;

      return row;
    });

    await appendRows(SHEET_ID, SHEET_NAME, newRows);
  }

  console.log("✅ Sync Complete");

  return {
    success: true,
    batchUpdated: batchUpdates.length,
    added: missing.length,
  };
}

// ---------------------------
// RUN WHEN EXECUTED DIRECTLY
// ---------------------------
(async () => {
  console.log("⏳ Running MF Sync locally...");
  try {
    const result = await syncMF();
    console.log("✔️ DONE:", result);
  } catch (err) {
    console.error("❌ ERROR:", err.message);
  }
})();
