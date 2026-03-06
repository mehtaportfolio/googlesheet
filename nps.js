import { google } from "googleapis";
import { createClient } from "@supabase/supabase-js";
import fetch from "node-fetch"; // for Node.js fetch
import dotenv from "dotenv";
dotenv.config();

console.log("⏳ Running NPS NAV Sync...");

// ---- GOOGLE SHEET AUTH ----
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
  const SHEET_NAME = "nps";
  const SUPABASE_TABLE = process.env.SUPABASE_TABLE_NPS || "nps_pension_fund_master";

  // ---- Read sheet data (starting from row 2, columns A to E) ----
  const sheetData = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2:E`,
  });

  const data = sheetData.data.values || [];
  if (!data.length) {
    console.log("No data found in sheet.");
    return;
  }

  const baseUrl = "https://www.npsnav.in/api/detailed/";
  
  // Prepare API requests in parallel
  const requests = data.map((row, i) => {
    const schemeCode = (row[1] || "").toString().trim();
    if (!schemeCode) return null;
    return { schemeCode, rowIndex: i };
  }).filter(req => req !== null);

  const responses = await Promise.all(
    requests.map(async (req) => {
      try {
        const res = await fetch(baseUrl + req.schemeCode);
        const navData = await res.json();
        return { ...req, navData };
      } catch (e) {
        console.log(`⚠️ Error fetching NAV for ${req.schemeCode}: ${e}`);
        return { ...req, error: e };
      }
    })
  );

  let supabasePayload = [];

  for (const res of responses) {
    if (res.error) continue;
    
    const rowIndex = res.rowIndex;
    const navData = res.navData;
    const latestNAV = parseFloat(navData["NAV"]);

    if (isNaN(latestNAV)) continue;

    const schemeName = data[rowIndex][0];
    const schemeCode = data[rowIndex][1];
    const currentCMP = parseFloat(data[rowIndex][2]);
    const fundName = data[rowIndex][4];

    // Update only if NAV changed
    if (latestNAV !== currentCMP) {
      data[rowIndex][3] = currentCMP || null; // LCP = old CMP
      data[rowIndex][2] = latestNAV;          // CMP = new NAV
    }

    supabasePayload.push({
      scheme_name: schemeName,
      scheme_code: schemeCode,
      cmp: data[rowIndex][2] || null,
      lcp: data[rowIndex][3] || null,
      fund_name: fundName || null
    });
  }

  // ---- Update sheet with new cmp + lcp ----
  await sheets.spreadsheets.values.update({
    spreadsheetId: SHEET_ID,
    range: `${SHEET_NAME}!A2`,
    valueInputOption: "RAW",
    requestBody: { values: data },
  });

  // ---- Upsert into Supabase ----
  if (supabasePayload.length > 0) {
    const { data: upsertRes, error } = await supabase
      .from(SUPABASE_TABLE)
      .upsert(supabasePayload, { onConflict: "scheme_code" });

    if (error) {
      console.log("❌ Supabase upsert error:", error);
    } else {
      console.log(`✅ Supabase sync completed: ${supabasePayload.length} rows`);
    }
  } else {
    console.log("No NAV data to sync.");
  }
}

// Run the sync locally
/*
fetchAndSyncNPSNAVs().then(() => console.log("✔️ Done"));
*/

// Export the main function for Node.js
export { fetchAndSyncNPSNAVs };
