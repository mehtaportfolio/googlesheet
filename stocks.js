import express from "express";
import { google } from "googleapis";
import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";
dotenv.config();

const app = express();

// --------- GOOGLE SHEETS AUTH ----------
const auth = new google.auth.JWT(
  process.env.GS_CLIENT_EMAIL,
  null,
  process.env.GS_PRIVATE_KEY.replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth });

// --------- SUPABASE ----------
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);

const norm = s => (s || "").toString().trim().toUpperCase();
const numOrNull = v => {
  if (!v) return null;
  const n = Number(v.toString().replace(/,/g, "").trim());
  return isNaN(n) ? null : n;
};

const isBlankOrPlaceholder = val => {
  if (!val) return true;
  const s = val.toString().trim().toUpperCase();
  return s === "" || s === "N/A" || s === "UNKNOWN";
};

async function getSheetValues() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEET_ID,
    range: `${process.env.GOOGLE_SHEET_NAME}!A1:Z10000`,
  });

  return res.data.values || [];
}

async function setValues(range, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values },
  });
}

// ---------------- MAIN SYNC FUNCTION ----------------
async function syncStocks() {
  console.log("🔄 Sync started...");

  // ---- Step 1: Fetch from Supabase ----
  const { data: supabaseRows, error } = await supabase
    .from(process.env.SUPABASE_TABLE_NAME)
    .select("stock_name, symbol, cmp, lcp, sector, industry, category");

  if (error) throw new Error(error.message);

  const supabaseSymbolsNorm = supabaseRows.map(r => norm(r.symbol));

  // ---- Step 2: Read Sheet ----
  const all = await getSheetValues();
  if (!all.length) return;

  const header = all[0];
  const rows = all.slice(1);

  const nameToRow = {};
  const symbolToRow = {};

  rows.forEach((r, i) => {
    const stockName = norm(r[0]);
    const symbol = norm(r[1]);
    if (stockName) nameToRow[stockName] = i + 2;
    if (symbol) symbolToRow[symbol] = i + 2;
  });

  const additions = [];

  // ---- Step 1: Supabase → Sheet Mapping ----
  for (const r of supabaseRows) {
    const stockName = norm(r.stock_name);
    const symbol = norm(r.symbol);

    if (!stockName || !symbol) continue;

    const rowBySymbol = symbolToRow[symbol];
    const rowByName = nameToRow[stockName];

    if (rowBySymbol) {
      const sheetRow = rows[rowBySymbol - 2];
      if (norm(sheetRow[0]) !== stockName)
        await setValues(`${process.env.GOOGLE_SHEET_NAME}!A${rowBySymbol}`, [[r.stock_name]]);

      if (!sheetRow[4] && r.sector)
        await setValues(`${process.env.GOOGLE_SHEET_NAME}!E${rowBySymbol}`, [[r.sector]]);

      if (!sheetRow[5] && r.industry)
        await setValues(`${process.env.GOOGLE_SHEET_NAME}!F${rowBySymbol}`, [[r.industry]]);

    } else if (rowByName && !rowBySymbol) {
      await setValues(`${process.env.GOOGLE_SHEET_NAME}!B${rowByName}`, [[symbol]]);
    } else {
      additions.push([r.stock_name, r.symbol]);
    }
  }

  // ---- Step: Append missing stocks ----
  if (additions.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.GOOGLE_SHEET_NAME}!A:B`,
      valueInputOption: "RAW",
      requestBody: { values: additions },
    });
  }

  // Refresh sheet after modifications
  const all2 = await getSheetValues();
  const rows2 = all2.slice(1).filter(r => r[1]);

  // ---- Step 2: Delete stocks NOT in Supabase ----
  const supSet = new Set(supabaseSymbolsNorm);
  for (let i = rows2.length - 1; i >= 0; i--) {
    if (!supSet.has(norm(rows2[i][1]))) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: process.env.GOOGLE_SHEET_ID,
        requestBody: { requests: [{ deleteDimension: { range: { sheetId: 0, dimension: "ROWS", startIndex: i + 1, endIndex: i + 2 } } }] }
      });
    }
  }

  // ---- Step 2B: Push category/sector/industry to Supabase ----
  const updatePayload = [];
  const supMap = Object.fromEntries(supabaseRows.map(r => [norm(r.symbol), r]));

  for (const r of rows2) {
    const symbol = norm(r[1]);
    const supRow = supMap[symbol];
    if (!supRow) continue;

    const updateObj = { symbol };

    if (r[4] && isBlankOrPlaceholder(supRow.sector)) updateObj.sector = r[4];
    if (r[5] && isBlankOrPlaceholder(supRow.industry)) updateObj.industry = r[5];
    if (r[6] !== supRow.category) updateObj.category = r[6];

    if (Object.keys(updateObj).length > 1) updatePayload.push(updateObj);
  }

  if (updatePayload.length)
    await supabase.from(process.env.SUPABASE_TABLE_NAME)
      .upsert(updatePayload, { onConflict: "symbol" });

  // ---- Step 3: Update Supabase CMP/LCP from sheet ----
  const refreshed = await getSheetValues();
  const newRows = refreshed.slice(1);

  const payload = newRows
    .filter(r => norm(r[1]) && supSet.has(norm(r[1])))
    .map(r => ({
      symbol: r[1],
      cmp: numOrNull(r[2]),
      lcp: numOrNull(r[3])
    }));

  if (payload.length)
    await supabase.from(process.env.SUPABASE_TABLE_NAME)
      .upsert(payload, { onConflict: "symbol" });

  console.log("✅ Sync completed.");
  return { success: true };
}

// ---- API TRIGGER ENDPOINT ----
app.get("/sync", async (_, res) => {
  try {
    const result = await syncStocks();
    res.json(result);
  } catch (err) {
    res.json({ error: err.message });
  }
});

// Export the main function for Node.js
export { syncStocks };
