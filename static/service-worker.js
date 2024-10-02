self.importScripts(
  "https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js",
  "https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.11.0/sql-wasm.js",
);

let db;
(async () => {
  const SQL = await initSqlJs({
    locateFile: (file) => `https://sql.js.org/dist/${file}`,
  });
  db = new SQL.Database();
})();

self.addEventListener("message", async (event) => {
  const { merchantID } = event.data;
  const response = await fetch(`/loader/${merchantID}`);
  if (response.ok) {
    const blob = await response.blob();
    const zip = await JSZip.loadAsync(blob);
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
      const header = rows[0];
      const columns = header
        .map((col, i) => {
          return `${col} ${inferSQLTypes(rows.slice(1, 2).map((row) => row[i]))}`;
        })
        .join(", ");
      const dataRows = rows.slice(1);

      db.run(`DROP TABLE IF EXISTS ${tableName};`);
      db.run(`CREATE TABLE IF NOT EXISTS ${tableName} (${columns});`);

      const numColumns = columns.split(",").length;
      const insertSQL = `INSERT INTO ${tableName} VALUES (${new Array(
        numColumns,
      )
        .fill("?")
        .join(", ")});`;

      for (let row of dataRows) {
        db.run(insertSQL, row);
      }
    }

    const binaryArray = db.export();
    postMessage(
      {
        action: "db-ready",
        buffer: binaryArray.buffer,
      },
      [binaryArray.buffer],
    );
  } else {
    postMessage({
      action: "error",
      message: "check server, response not ok when fetching merchant data",
    });
  }
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
