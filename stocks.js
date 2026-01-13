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

const isBlankOrPlaceholder = val => {
  if (!val) return true;
  const s = val.toString().trim().toUpperCase();
  return s === "" || s === "N/A" || s === "UNKNOWN";
};

// ---------------- SHEET HELPERS ----------------
async function getSheetValues() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.GOOGLE_SHEET_ID,
    range: `${process.env.GOOGLE_SHEET_NAME}!A1:Z20000`
  });
  return res.data.values || [];
}

async function setValues(range, values) {
  await sheets.spreadsheets.values.update({
    spreadsheetId: process.env.GOOGLE_SHEET_ID,
    range,
    valueInputOption: "RAW",
    requestBody: { values }
  });
}

// ---------------- MAIN SYNC FUNCTION ----------------
async function syncStocks() {
  console.log("🔄 Sync started...");

  // ---------- 1) FETCH SUPABASE ----------
  const { data: supabaseRows, error } = await supabase
    .from(process.env.SUPABASE_TABLE_NAME)
    .select(
      "stock_name,symbol,cmp,lcp,macro_sector,sector,known_sector,industry,basic_industry,category"
    );

  if (error) throw new Error(error.message);

  const supSet = new Set(supabaseRows.map(r => norm(r.symbol)));
  const supMap = Object.fromEntries(
    supabaseRows.map(r => [norm(r.symbol), r])
  );

  // ---------- 2) READ SHEET ----------
  const all = await getSheetValues();
  if (all.length <= 1) return;

  const rows = all.slice(1);

  const nameToRow = {};
  const symbolToRow = {};

  rows.forEach((r, i) => {
    if (r[0]) nameToRow[norm(r[0])] = i + 2;
    if (r[1]) symbolToRow[norm(r[1])] = i + 2;
  });

  const additions = [];

  // ---------- 3) SUPABASE → SHEET (NAME / SYMBOL ONLY) ----------
  for (const r of supabaseRows) {
    const stockName = norm(r.stock_name);
    const symbol = norm(r.symbol);
    if (!stockName || !symbol) continue;

    const rowBySymbol = symbolToRow[symbol];
    const rowByName = nameToRow[stockName];

    if (rowBySymbol) {
      const sheetRow = rows[rowBySymbol - 2];
      if (norm(sheetRow[0]) !== stockName) {
        await setValues(
          `${process.env.GOOGLE_SHEET_NAME}!A${rowBySymbol}`,
          [[r.stock_name]]
        );
      }
    } else if (rowByName) {
      await setValues(
        `${process.env.GOOGLE_SHEET_NAME}!B${rowByName}`,
        [[symbol]]
      );
    } else {
      additions.push([r.stock_name, r.symbol]);
    }
  }

  if (additions.length) {
    await sheets.spreadsheets.values.append({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.GOOGLE_SHEET_NAME}!A:B`,
      valueInputOption: "RAW",
      requestBody: { values: additions }
    });
  }

  // ---------- REFRESH SHEET ----------
  const refreshed = await getSheetValues();
  const rows2 = refreshed.slice(1).filter(r => r[1]);

  // ---------- 4) DELETE ROWS NOT IN SUPABASE ----------
  for (let i = rows2.length - 1; i >= 0; i--) {
    if (!supSet.has(norm(rows2[i][1]))) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId: process.env.GOOGLE_SHEET_ID,
        requestBody: {
          requests: [{
            deleteDimension: {
              range: {
                sheetId: 0,
                dimension: "ROWS",
                startIndex: i + 1,
                endIndex: i + 2
              }
            }
          }]
        }
      });
    }
  }

  // ---------- 5) SAFE BATCH SUPABASE → SHEET (NO FORMULAS) ----------
  const macroSectorCol = [];
  const sectorCol = [];
  const knownSectorCol = [];
  const industryCol = [];
  const basicIndustryCol = [];

  rows2.forEach(r => {
    const sup = supMap[norm(r[1])] || {};
    macroSectorCol.push([sup.macro_sector || r[4]]);
    sectorCol.push([sup.sector || r[5]]);
    knownSectorCol.push([sup.known_sector || r[6]]);
    industryCol.push([sup.industry || r[7]]);
    basicIndustryCol.push([sup.basic_industry || r[8]]);
  });

  const startRow = 2;
  const rowCount = rows2.length;

  await setValues(`${process.env.GOOGLE_SHEET_NAME}!E${startRow}`, macroSectorCol);
  await setValues(`${process.env.GOOGLE_SHEET_NAME}!F${startRow}`, sectorCol);
  await setValues(`${process.env.GOOGLE_SHEET_NAME}!G${startRow}`, knownSectorCol);
  await setValues(`${process.env.GOOGLE_SHEET_NAME}!H${startRow}`, industryCol);
  await setValues(`${process.env.GOOGLE_SHEET_NAME}!I${startRow}`, basicIndustryCol);

  // ---------- 6) SHEET → SUPABASE (CATEGORY ONLY) ----------
  const updatePayload = [];

  rows2.forEach(r => {
    const symbol = norm(r[1]);
    const supRow = supMap[symbol];
    if (supRow && r[9] !== supRow.category) {
      updatePayload.push({ symbol, category: r[9] });
    }
  });

  if (updatePayload.length) {
    await supabase
      .from(process.env.SUPABASE_TABLE_NAME)
      .upsert(updatePayload, { onConflict: "symbol" });
  }

  // ---------- 7) CMP / LCP UPDATE ----------
  const cmpPayload = rows2
    .filter(r => supSet.has(norm(r[1])))
    .map(r => ({
      symbol: r[1],
      cmp: numOrNull(r[2]),
      lcp: numOrNull(r[3])
    }));

  if (cmpPayload.length) {
    await supabase
      .from(process.env.SUPABASE_TABLE_NAME)
      .upsert(cmpPayload, { onConflict: "symbol" });
  }

  console.log("✅ Sync completed.");
  return { success: true };
}

// ---------- API ENDPOINT ----------
app.get("/sync", async (_, res) => {
  try {
    res.json(await syncStocks());
  } catch (err) {
    res.json({ error: err.message });
  }
});

export { syncStocks };
