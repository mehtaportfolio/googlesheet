import { google } from "googleapis";
import { createClient } from "@supabase/supabase-js";
import fetch from "node-fetch";
import dotenv from "dotenv";
dotenv.config();

// --------- CONFIG ----------
const serviceAccount = JSON.parse(
  Buffer.from(process.env.GS_JSON_BASE64, "base64").toString("utf-8")
);

const auth = new google.auth.JWT(
  serviceAccount.client_email,
  null,
  serviceAccount.private_key,
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth });

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_API_KEY
);

// ---------------- HELPERS ----------------
const norm = s => (s || "").toString().trim().toUpperCase();

const numOrNull = v => {
  if (v === "" || v === null || v === undefined) return null;
  const n = Number(v.toString().replace(/,/g, "").trim());
  return isNaN(n) ? null : n;
};

// ---------------- TRIGGER EXTERNAL UPDATER ----------------
async function triggerYahooFinanceUpdater() {
  const renderUrl = "https://stock-yahoo.onrender.com/fill-missing-cmp";
  try {
    const response = await fetch(renderUrl);
    console.log("✅ Yahoo Finance updater triggered. Status:", response.status);
  } catch (err) {
    console.log("❌ Failed to trigger Render backend:", err.message);
  }
}

// ---------------- MAIN SYNC FUNCTION ----------------
async function syncStocks() {
  console.log("🔄 Starting Google Sheet ↔ Supabase sync...");

  try {
    // STEP 1: FETCH ALL SUPABASE ROWS (PAGINATED)
    let supRows = [];
    let from = 0;
    const batchSize = 1000;

    while (true) {
      const { data, error } = await supabase
        .from(process.env.SUPABASE_TABLE_NAME)
        .select("stock_name,symbol")
        .range(from, from + batchSize - 1);

      if (error) throw new Error("Supabase fetch failed: " + error.message);
      if (!data || data.length === 0) break;

      supRows = supRows.concat(data);
      if (data.length < batchSize) break;
      from += batchSize;
    }

    // STEP 2: READ SHEET ONCE
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.GOOGLE_SHEET_NAME}!A:J`
    });
    let sheetData = res.data.values || [];

    const sheetMap = {};
    sheetData.forEach((r, i) => {
      const symbol = norm(r[1]);
      if (symbol) sheetMap[symbol] = i;
    });

    const supSymbolSet = new Set();

    // STEP 3: APPLY SUPABASE → SHEET IN MEMORY
    let nextEmptyIndex = 0;
    const findNextEmpty = () => {
      while (nextEmptyIndex < sheetData.length && norm(sheetData[nextEmptyIndex][1])) {
        nextEmptyIndex++;
      }
      return nextEmptyIndex;
    };

    supRows.forEach(r => {
      const symbol = norm(r.symbol);
      const name = r.stock_name;
      if (!symbol) return;

      supSymbolSet.add(symbol);

      if (sheetMap.hasOwnProperty(symbol)) {
        const index = sheetMap[symbol];
        if (!name) {
          sheetData[index] = Array(10).fill("");
        } else {
          sheetData[index][0] = name;
          sheetData[index][1] = symbol;
        }
      } else if (name) {
        const index = findNextEmpty();
        if (index < sheetData.length) {
          sheetData[index][0] = name;
          sheetData[index][1] = symbol;
        } else {
          const newRow = new Array(10).fill("");
          newRow[0] = name;
          newRow[1] = symbol;
          sheetData.push(newRow);
        }
        sheetMap[symbol] = index; // Mark as taken
      }
    });

    // STEP 4: WRITE ONLY NAME + SYMBOL (DO NOT TOUCH FORMULAS)
    if (sheetData.length > 0) {
      const nameSymbolData = sheetData.map(r => [r[0] || "", r[1] || ""]);
      await sheets.spreadsheets.values.update({
        spreadsheetId: process.env.GOOGLE_SHEET_ID,
        range: `${process.env.GOOGLE_SHEET_NAME}!A2`,
        valueInputOption: "RAW",
        requestBody: { values: nameSymbolData }
      });
    }

    // STEP 5: SHEET → SUPABASE (UPSERT BULK)
    const payload = sheetData
      .filter(r => r[1])
      .map(r => ({
        symbol: norm(r[1]),
        cmp: numOrNull(r[2]),
        lcp: numOrNull(r[3]),
        category: r[4] || null,
        market_cap: numOrNull(r[5])
      }));

    if (payload.length > 0) {
      const { error: upsertError } = await supabase
        .from(process.env.SUPABASE_TABLE_NAME)
        .upsert(payload, { onConflict: "symbol" });

      if (upsertError) throw new Error("Supabase upsert failed: " + upsertError.message);
    }

    console.log("🚀 ULTRA SUPER FAST SYNC COMPLETED");
    
    // Trigger the external updater just like the Apps Script does
    await triggerYahooFinanceUpdater();

    return { success: true, rowsSynced: sheetData.length };

  } catch (err) {
    console.log("❌ SYNC ERROR: " + err.message);
    throw err;
  }
}

export { syncStocks };
