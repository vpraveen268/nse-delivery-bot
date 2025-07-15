const https = require("https");
const { GoogleSpreadsheet } = require("google-spreadsheet");
const csv = require("csv-parser");
const creds = JSON.parse(process.env.GOOGLE_CREDENTIALS);
const SHEET_ID = "1b9s-_Dvy4DdrO4DSo-ADcSUm1Zxz1UK2FiIhYdXr5e8"; // your sheet ID
const axios = require("axios"); // Add this at top
const zlib = require('zlib');

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
        .pipe(zlib.createGunzip())  // Add this if response is gzip
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
  const sheet = doc.sheetsByIndex[0];
  const rows = await sheet.getRows();

  console.log("📊 Total rows in Google Sheet:", rows.length);
  console.log("📄 Total rows in CSV Bhavcopy:", csvData.length);

  // Show first 10 symbols from CSV
  //console.log("📄 First 10 SYMBOLs from CSV:");
  //console.log(csvData.slice(0, 10).map(row => row["SYMBOL"]));

  // Build delivery map from CSV
  const deliveryMap = {};
  for (const row of csvData) {
    const symbol = row["SYMBOL"];
    const delivery = row["DELIV_PER"];
    if (symbol && delivery) {
      deliveryMap[symbol.trim().toUpperCase()] = delivery.trim();
    }
  }

  // Show first 10 symbols from delivery map
  //console.log("🗺️ Sample keys in deliveryMap:", Object.keys(deliveryMap).slice(0, 10));

  // Compare with Google Sheet symbols
  //console.log("📋 First 10 symbols from Google Sheet:");
  //console.log(rows.slice(0, 10).map(r => r._rawData[0]));

  // Update Sheet
  for (const row of rows) {
    const rawSymbol = row._rawData[0];
    const symbol = rawSymbol?.toUpperCase().trim();

    if (symbol && deliveryMap[symbol]) {
      row._rawData[1] = deliveryMap[symbol]; // Column B (index 1)
      await row.save();
      console.log(`✅ Updated ${symbol} → ${deliveryMap[symbol]}`);
    } else {
      console.log(`⚠️  Symbol '${symbol}' not found in Bhavcopy`);
    }
  }
}

async function main() {
  try {
    const dateStr = getTodayDateString();
    const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_${dateStr}.csv`;
    //const url = `https://nsearchives.nseindia.com/products/content/sec_bhavdata_full_14072025.csv`;
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
