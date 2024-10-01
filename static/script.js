document.addEventListener("DOMContentLoaded", () => {
  const telemetry = document.getElementById("telemetry-data");

  let db;
  const loadSQLite = async () => {
    const SQL = await window.initSqlJs({
      locateFile: (file) => `https://sql.js.org/dist/${file}`,
    });
    db = new SQL.Database();
    window.db = db;
    console.info("SQLite instance loaded and ready to be used - see window.db");
  };
  loadSQLite();

  const pollTelemetry = async () => {
    const response = await fetch("/telemetry");
    if (response.ok) {
      const data = await response.json();
      telemetry.innerHTML = "";

      for (const [key, value] of Object.entries(data)) {
        const latest = value[value.length - 1];
        const p = document.createElement("p");
        switch (latest.value) {
          case false:
            continue;
          case true:
            p.textContent = `Marked ${key}...`;
            break;
          default:
            if (typeof latest.value === "number") {
              p.textContent = `${key}: ${latest.value.toLocaleString()}`;
            } else {
              p.textContent = `${key}: ${latest.value}`;
            }
            break;
        }

        telemetry.appendChild(p);
      }
    } else {
      console.error("error fetching diagnostics data");
    }
  };

  const generateButton = document.getElementById("generate-button");
  generateButton.addEventListener("click", async () => {
    generateButton.classList.add("opacity-50", "pointer-events-none");
    try {
      await fetch("/generate", {
        method: "POST",
      });
    } catch (error) {
      console.error("error sending post request:", error);
    } finally {
      generateButton.classList.remove("opacity-50", "pointer-events-none");
    }
  });

  const loadMerchantButton = document.getElementById("load-merchant-button");
  loadMerchantButton.addEventListener("click", async () => {
    try {
      const merchantID = "d773d571-7fce-4aa6-ad04-05e37a93fb26";
      const response = await fetch(`/loader/${merchantID}`);
      if (response.ok) {
        const blob = await response.blob();

        const zip = await window.JSZip.loadAsync(blob);
        const files = [];

        zip.forEach((relativePath, file) => {
          if (relativePath.endsWith(".csv")) {
            files.push(file);
          }
        });

        for (const file of files) {
          const csvContent = await file.async("string");
          const rows = csvContent.split("\n").map((row) => row.split(","));

          const tableName = file.name.replace(".csv", "");
          const header = rows[0]; // header has column names
          const columns = header
            .map((col, i) => {
              return `${col} ${inferSQLTypes(rows.slice(1, 2).map((row) => row[i]))}`;
            })
            .join(", ");

          db.run(`DROP TABLE IF EXISTS ${tableName};`);
          db.run(`CREATE TABLE ${tableName} (${columns});`);
          console.info(
            `${tableName} dropped and recreated with columns: ${columns}`,
          );

          const insertSQL = `INSERT INTO ${tableName} VALUES (${new Array(rows[0].length).fill("?").join(", ")});`;
          for (let i = 1; i < rows.length; i++) {
            // row 0 is header
            const row = rows[i];
            try {
              db.run(insertSQL, row);
            } catch (error) {
              console.error(
                `error inserting row into ${tableName}:`,
                row,
                error,
              );
            }
          }
          console.info(`${tableName} loaded with ${rows.length - 1} rows`);
        }
      } else {
        console.error("failed to fetch merchant data");
      }
    } catch (error) {
      console.error("error loading merchant data:", error);
    }
  });

  setInterval(pollTelemetry, 1000);
});

// inferSQLTypes infers the SQL data type for a given sample of values.
// It checks the values and asserts a type hierarchy: INTEGER > REAL > TEXT.
function inferSQLTypes(sample) {
  let isInteger = true;
  let isReal = true;

  for (let value of sample) {
    const number = Number(value);
    if (isNaN(number)) {
      isReal = false;
      isInteger = false;
      break;
    }

    if (!Number.isInteger(number)) {
      isInteger = false;
    }
  }

  if (isInteger) return "INTEGER";
  if (isReal) return "REAL";
  return "TEXT";
}
