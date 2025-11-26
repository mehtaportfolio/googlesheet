import { google } from "googleapis";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

// ---- GOOGLE AUTH ----
const auth = new google.auth.JWT(
  process.env.GS_CLIENT_EMAIL,
  null,
  process.env.GS_PRIVATE_KEY.replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth });

// ---- SUPABASE ----
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);

const norm = s => (s || "").toString().trim().toUpperCase();
const numOrNull = v => {
  if (!v) return null;
  const n = Number(v.toString().replace(/,/g, "").trim());
  return isNaN(n) ? null : n;
};

// ---- Helper to read sheet ----
async function readSheet(sheetId, range) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range,
  });
  return res.data.values || [];
}

// ---- Helper to write multiple rows ----
async function writeSheet(sheetId, range, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: sheetId,
    range,
    valueInputOption: "RAW",
    requestBody: { values },
  });
}

// ---- Helper append ----
async function appendRows(sheetId, sheetName, rows) {
  await sheets.spreadsheets.values.append({
    spreadsheetId: sheetId,
    range: `${sheetName}!A1`,
    valueInputOption: "RAW",
    requestBody: { values: rows },
  });
}

// ---- MAIN SYNC FUNCTION ----
export async function syncMutualFunds() {
  console.log("🔄 MF Sync Started...");

  const SHEET_ID = process.env.GOOGLE_SHEET_ID;
  const SHEET_NAME = process.env.GOOGLE_SHEET_NAME_MF;
  const TABLE_NAME = process.env.SUPABASE_TABLE_MF;

  // ---- Fetch Supabase Rows ----
  const { data: supabaseRows, error } = await supabase
    .from(TABLE_NAME)
    .select("isin,scheme_code,fund_full_name,cmp,lcp");

  if (error) throw new Error(error.message);

  const supabaseMap = {};
  supabaseRows.forEach(r => (supabaseMap[norm(r.isin)] = r));

  // ---- Read Google Sheet ----
  const all = await readSheet(SHEET_ID, `${SHEET_NAME}!A1:Z10000`);
  const headers = all[0];
  const rows = all.slice(1);

  const headerMap = {};
  headers.forEach((h, i) => (headerMap[norm(h)] = i));

  const isinIndex = headerMap["ISIN"];
  const navIndex = headerMap["NAV"];
  const schemeNameIndex = headerMap["SCHEME NAME"];
  const schemeCodeIndex = headerMap["SCHEME CODE"];

  const googleMap = {};
  rows.forEach(r => {
    const isin = norm(r[isinIndex]);
    const nav = numOrNull(r[navIndex]);
    if (isin) googleMap[isin] = { nav, row: r };
  });

  const lcpUpdates = [];
  const cmpUpdates = [];
  const missingRows = [];

  // ---- Prepare Sync Operations ----
  for (const r of supabaseRows) {
    const isin = norm(r.isin);
    if (!isin) continue;

    // (A) Update LCP ← CMP (Always)
    lcpUpdates.push({ isin: r.isin, lcp: r.cmp });

    // (B) CMP ← NAV from sheet
    if (googleMap[isin] && googleMap[isin].nav != null && googleMap[isin].nav !== r.cmp) {
      cmpUpdates.push({ isin: r.isin, cmp: googleMap[isin].nav });
    }

    // (C) Missing ISIN -> Add to Sheet
    if (!googleMap[isin]) {
      missingRows.push({
        isin: r.isin,
        name: r.fund_full_name || "",
        scheme_code: r.scheme_code || "",
      });
    }
  }

  // ---- Step 4: Batch update LCP ----
  if (lcpUpdates.length) {
    for (const row of lcpUpdates) {
      await supabase.from(TABLE_NAME)
        .update({ lcp: row.lcp })
        .eq("isin", row.isin);
    }
  }

  // ---- Step 5: Batch update CMP ----
  if (cmpUpdates.length) {
    for (const row of cmpUpdates) {
      await supabase.from(TABLE_NAME)
        .update({ cmp: row.cmp })
        .eq("isin", row.isin);
    }
  }

  // ---- Step 6: Append Missing Rows ----
  if (missingRows.length > 0) {
    const newRows = missingRows.map(r => {
      const row = new Array(headers.length).fill("");
      if (schemeCodeIndex !== undefined) row[schemeCodeIndex] = r.scheme_code;
      if (schemeNameIndex !== undefined) row[schemeNameIndex] = r.name;
      if (isinIndex !== undefined) row[isinIndex] = r.isin;
      return row;
    });

    await appendRows(SHEET_ID, SHEET_NAME, newRows);
  }

  console.log("✅ MF Sync Completed.");

  return {
    added: missingRows.length,
    lcp: lcpUpdates.length,
    cmp: cmpUpdates.length,
    success: true
  };
}

// ---- Run locally for testing ----
(async () => {
  console.log("⏳ Running MF Sync locally...");
  try {
    const result = await syncMutualFunds();
    console.log("✔️ Done:", result);
  } catch (err) {
    console.error("❌ Error:", err);
  }
})();
