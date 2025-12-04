import fs from "fs";
import { google } from "googleapis";
import dotenv from "dotenv";
dotenv.config();

// Load key from PEM
const privateKey = fs.readFileSync("./service-account-key.pem", "utf8");

const auth = new google.auth.JWT(
  process.env.GS_CLIENT_EMAIL,
  null,
  privateKey,
  ["https://www.googleapis.com/auth/spreadsheets"]
);

const sheets = google.sheets({ version: "v4", auth });

async function testSheets() {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: process.env.GOOGLE_SHEET_ID,
      range: `${process.env.GOOGLE_SHEET_NAME_MF}!A1:B5`
    });
    console.log("✅ Connected successfully:", res.data.values);
  } catch (err) {
    console.error("❌ Sheets auth failed:", err.response?.data || err.message);
  }
}

testSheets();
