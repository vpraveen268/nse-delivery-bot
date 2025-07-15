const https = require("https");
const { GoogleSpreadsheet } = require("google-spreadsheet");
const csv = require("csv-parser");
const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
const SHEET_ID = "1b9s-_Dvy4DdrO4DSo-ADcSUm1Zxz1UK2FiIhYdXr5e8"; // your sheet ID
const axios = require("axios"); // Add this at top


function getTodayDateString() {
  const now = new Date();
  const dd = String(now.getDate()).padStart(2, "0");
  const mm = String(now.getMonth() + 1).padStart(2, "0");
  const yyyy = now.getFullYear();
  return `${dd}${mm}${yyyy}`;
}


function downloadCSV(url) {
  return new Promise(async (resolve, reject) => {
    const results = [];

    try {
      const response = await axios.get(url, {
        headers: {
  "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
  "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
  "Accept-Encoding": "gzip, deflate, br",
  "Accept-Language": "en-US,en;q=0.9",
  "Referer": "https://www.nseindia.com/",
  "Origin": "https://www.nseindia.com",
  "Connection": "keep-alive",
  "Host": "nsearchives.nseindia.com"
},
        timeout: 20000, // 20 seconds
        responseType: "stream"
      });

      //console.log("⏱ HTTP Status Code:", response.status);

      let headerLogged = false;

      response.data
        .pipe(csv())
        .on("data", (rawData) => {
          const cleanData = {};
          for (const key in rawData) {
            const cleanKey = key.replace(/\uFEFF/g, "").trim(); // Remove BOM & whitespace
            cleanData[cleanKey] = rawData[key]?.trim();
          }

          if (!headerLogged) {
            //console.log("🧾 Keys in CSV row object:", Object.keys(cleanData));
            headerLogged = true;
          }

          results.push(cleanData);
        })
        .on("end", () => resolve(results))
        .on("error", (err) => reject(err));

    } catch (err) {
      reject(err);
    }
  });
}



async function updateSheetFromCSV(csvData) {
  const doc = new GoogleSpreadsheet(SHEET_ID);
  await doc.useServiceAccountAuth(creds);
  await doc.loadInfo();

  // Map column index: SYMBOL, DAY1 → DAY31 = columns 0 to 31
  const todayColIndex = new Date().getDate(); // 1 → 31

  // Build lookup maps from CSV
  const deliveryMap = {};
  const closeMap = {};
  const volumeMap = {};

  for (const row of csvData) {
    const symbol = row["SYMBOL"]?.trim().toUpperCase();
    if (!symbol) continue;

    if (row["DELIV_PER"]) deliveryMap[symbol] = row["DELIV_PER"].trim();
    if (row["CLOSE_PRICE"]) closeMap[symbol] = row["CLOSE_PRICE"].trim();
    if (row["TURNOVER_LACS"]) volumeMap[symbol] = row["TURNOVER_LACS"].trim();
  }

  const updateSheet = async (sheetName, dataMap) => {
    const sheet = doc.sheetsByTitle[sheetName];
    if (!sheet) {
      console.error(`❌ Sheet '${sheetName}' not found`);
      return;
    }

    const rows = await sheet.getRows();
    console.log(`📄 Updating '${sheetName}' → Rows: ${rows.length}`);

    for (const row of rows) {
      const rawSymbol = row._rawData[0];
      const symbol = rawSymbol?.trim().toUpperCase();
      const value = dataMap[symbol];

      if (symbol && value) {
        row._rawData[todayColIndex] = value;
        await row.save();
        console.log(`✅ [${sheetName}] ${symbol} → ${value}`);
      } else {
        console.log(`⚠️  [${sheetName}] ${symbol} not found in Bhavcopy`);
      }
    }
  };

  // Update all three sheets
  await updateSheet("DelPerc", deliveryMap);
  await updateSheet("ClosePrice", closeMap);
  await updateSheet("Volume", volumeMap);
}


async function main() {
  try {
    const dateStr = getTodayDateString();
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${dateStr}.csv`;
    const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_14072025.csv`;
    console.log("📥 Downloading CSV:", url);

    const csvData = await downloadCSV(url);
    console.log("📊 Records downloaded:", csvData.length);

    await updateSheetFromCSV(csvData);
    console.log("✅ Sheet updated successfully.");
  } catch (error) {
    console.error("❌ Error:", error.message);
  }
}

main();
