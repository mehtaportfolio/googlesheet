import { google } from "googleapis";
import { createClient } from "@supabase/supabase-js";
import fetch from "node-fetch"; // for Node.js fetch
import dotenv from "dotenv";
dotenv.config();

console.log("⏳ Running NPS NAV Sync...");

// ---- GOOGLE SHEET AUTH ----
const auth = new google.auth.JWT(
  process.env.GS_CLIENT_EMAIL,
  null,
  process.env.GS_PRIVATE_KEY.replace(/\\n/g, "\n"),
  ["https://www.googleapis.com/auth/spreadsheets"]
);
const sheets = google.sheets({ version: "v4", auth });

// ---- SUPABASE CLIENT ----
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_API_KEY);

const norm = s => (s || "").toString().trim().toUpperCase();
const numOrNull = v => {
  if (!v) return null;
  const n = Number(v.toString().replace(/,/g, "").trim());
  return isNaN(n) ? null : n;
};

async function fetchAndSyncNPSNAVs() {
  const SHEET_ID = process.env.GOOGLE_SHEET_ID;
  const SHEET_NAME = "nps"; // Sheet name
  const SUPABASE_TABLE = process.env.SUPABASE_TABLE_NPS;

  // ---- Read sheet data ----
  const sheetData = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2:E1000`,
  });

  const data = sheetData.data.values || [];
  if (!data.length) {
    console.log("No data found in sheet.");
    return;
  }

  const baseUrl = "https://www.npsnav.in/api/detailed/";
  let supabasePayload = [];

  // 1️⃣ Copy cmp → lcp in memory
  for (let row of data) {
    if (row[2]) row[3] = row[2]; // cmp → lcp
  }

  // 2️⃣ Fetch latest NAVs
  for (let i = 0; i < data.length; i++) {
    const schemeName = data[i][0];
    const schemeCode = data[i][1];
    const cmp = data[i][2];
    const lcp = data[i][3];
    const fundName = data[i][4];

    if (!schemeCode || schemeCode.trim() === "") {
      console.log(`⏭️ Skipping empty scheme code at row ${i + 2}`);
      continue;
    }

    try {
      const res = await fetch(baseUrl + schemeCode);
      const navData = await res.json();
      const latestCmp = parseFloat(navData["NAV"]);
      if (isNaN(latestCmp)) throw new Error("Invalid NAV response");

      // update cmp in memory
      data[i][2] = latestCmp;

      // prepare Supabase row
      supabasePayload.push({
        scheme_name: schemeName,
        scheme_code: schemeCode,
        cmp: isNaN(latestCmp) ? null : latestCmp,
        lcp: isNaN(lcp) || lcp === "" ? null : parseFloat(lcp),
        fund_name: fundName || null,
      });
    } catch (e) {
      console.log(`⚠️ Error fetching NAV for ${schemeCode}: ${e}`);
    }
  }

  // 3️⃣ Update sheet with new cmp + lcp
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2`,
    valueInputOption: "RAW",
    requestBody: { values: data },
  });

  // 4️⃣ Upsert into Supabase
  if (supabasePayload.length > 0) {
    const { data: upsertRes, error } = await supabase
      .from(SUPABASE_TABLE)
      .upsert(supabasePayload, { onConflict: ["scheme_code"] });

    if (error) {
      console.log("❌ Supabase upsert error:", error);
    } else {
      const rowCount = Array.isArray(upsertRes) ? upsertRes.length : 0;
      console.log(`✅ Supabase upsert successful: ${rowCount} rows`);
    }
  } else {
    console.log("No NAV data to sync.");
  }
}

// Run the sync locally
fetchAndSyncNPSNAVs().then(() => console.log("✔️ Done"));

// Export the main function for Node.js
export { fetchAndSyncNPSNAVs };
