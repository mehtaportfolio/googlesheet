// server.js
import express from "express";
import dotenv from "dotenv";
dotenv.config();

import { syncMutualFunds } from "./mf.js";
import { fastNAVUpdate } from "./amfi.js";
import { syncStocks } from "./stocks.js"; // assuming your first script is stocks.js
import { fetchAndSyncNPSNAVs } from "./nps.js";

const app = express();
const PORT = process.env.PORT || 3000;

app.get("/", (req, res) => {
  res.send(`
    <h2>🚀 Google Sheet Sync Service Running</h2>
    <p>Available Endpoints:</p>
    <ul>
      <li><a href="/mf">/mf</a> - Sync Mutual Funds</li>
      <li><a href="/amfi">/amfi</a> - Sync AMFI NAV</li>
      <li><a href="/stocks">/stocks</a> - Sync Stock Prices</li>
      <li><a href="/nps">/nps</a> - Sync NPS Data</li>
    </ul>
  `);
});


// ---- Mutual Funds ----
app.get("/mf", async (req, res) => {
  try {
    const result = await syncMutualFunds();
    res.json({ status: "success", result });
  } catch (err) {
    res.json({ status: "error", message: err.toString() });
  }
});

// ---- AMFI FAST NAV ----
app.get("/amfi", async (req, res) => {
  try {
    const sheetName = req.query.sheet || "MF";
    const result = await fastNAVUpdate(sheetName);
    res.json({ status: "success", result });
  } catch (err) {
    res.json({ status: "error", message: err.toString() });
  }
});

// ---- Stocks Sync ----
app.get("/stocks", async (req, res) => {
  try {
    const result = await syncStocks();
    res.json({ status: "success", result });
  } catch (err) {
    res.json({ status: "error", message: err.toString() });
  }
});

// ---- NPS NAV Sync ----
app.get("/nps", async (req, res) => {
  try {
    await fetchAndSyncNPSNAVs();
    res.json({ status: "success", message: "NPS NAVs synced successfully" });
  } catch (err) {
    res.json({ status: "error", message: err.toString() });
  }
});

app.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
