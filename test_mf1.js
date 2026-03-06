import { google } from "googleapis";
import dotenv from "dotenv";
dotenv.config();

const SHEET_ID = process.env.GOOGLE_SHEET_ID;

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

async function run() {
  const sheets = await getSheetsClient();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: SHEET_ID,
    range: "MF1!A1:Z5"
  });
  console.log(JSON.stringify(res.data.values, null, 2));
}

run().catch(console.error);
