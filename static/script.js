let currentDownloadMerchantID = null;

(async () => {
  const duckdb = await import('https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.28.1-dev106.0/+esm');

  // https://duckdb.org/docs/api/wasm/instantiation
  const bundle = await duckdb.selectBundle(duckdb.getJsDelivrBundles());
  const worker_url = URL.createObjectURL(
    new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
  );

  const worker = new Worker(worker_url);
  const logger = new duckdb.ConsoleLogger();
  const db = new duckdb.AsyncDuckDB(logger, worker);

  await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
  URL.revokeObjectURL(worker_url);

  const conn = await db.connect();
  console.log(conn)

  function init() {
    const telemetry = document.getElementById("telemetry-data");
    const merchantList = document.getElementById("merchant-list");
    const generateButton = document.getElementById("generate-button");


    setInterval(async () => {
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
      }
    }, 1000);

    generateButton.addEventListener("click", async () => {
      generateButton.classList.add("opacity-50", "pointer-events-none");
      try {
        const response = await fetch("/generate", { method: "POST" });
        if (response.ok) {
          const data = await response.json();
          console.log(`${data.Transactions.toLocaleString()} transactions, ${data.Lines.toLocaleString()} transaction lines - over ${data.Merchants.length.toLocaleString()} merchants`);
          data.Merchants.forEach((merchant) => {
            const p = document.createElement("p");
            p.classList.add("flex", "items-center", "mb-2");
            p.dataset.merchantId = merchant.ID.toString();

            const span = document.createElement("span");
            span.textContent = merchant.Name;

            let icon = analyticsIcon(merchant);
            icon.addEventListener("click", async () => {
              runAnalytics(merchant.ID.toString());
            });
            p.appendChild(icon);
            p.appendChild(span);

            icon = downloadIcon(merchant.ID.toString());
            icon.addEventListener("click", async () => {
              const merchantID = merchant.ID.toString();
              currentDownloadMerchantID = merchantID;
              icon.classList.add("hidden");
              restoreDownloadIcons();

              try {
                const response = await fetch(`/loader/${merchantID}`);
                if (!response.ok) {
                  console.error("Response not OK when loading merchant");
                  return;
                }

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
                  const tableName = file.name.replace(".csv", "");

                  await db.registerFileText(`/${file.name}`, csvContent);
                  await conn.query(`
                    DROP TABLE IF EXISTS ${tableName};
                    CREATE TABLE ${tableName} AS SELECT * FROM read_csv_auto('/${file.name}', HEADER=TRUE, SAMPLE_SIZE=-1);
                  `);
                }

                console.info(`Database loaded for ${merchantID}`);
              } catch (error) {
                console.error("Error loading data into DuckDB:", error);
              }
            });

            p.appendChild(icon);

            merchantList.appendChild(p);
          });
        } else {
          console.error("error in response from /generate:", response.statusText);
        }
      } catch (error) {
        console.error("error sending POST request:", error);
      } finally {
        generateButton.classList.remove("opacity-50", "pointer-events-none");
      }
    });

    const runAnalytics = async (merchantID) => {
      if (currentDownloadMerchantID === merchantID) {
        try {
          const query = `SELECT
          p.id AS product_id,
          p.name AS product_name,
          SUM(p.price_cents * tl.quantity) AS total_revenue
        FROM main_products p
        JOIN main_transaction_lines tl ON p.id = tl.product_id
        GROUP BY p.id, p.name
        ORDER BY total_revenue DESC, product_name ASC
        LIMIT 5;`;

          const result = await conn.query(query);
          console.log(JSON.parse(JSON.stringify(result.toArray())));

        } catch (error) {
          console.error("Error executing query:", error);
        }
      } else {
        try {
          const response = await fetch(`/analytics/${merchantID}`);
          const data = await response.json();
          console.log("Server response:", data);
        } catch (error) {
          console.error("Error fetching analytics:", error);
        }
      }
    };
  }

  if (document.readyState === 'loading') {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }

  // downloadIcon DOM API builds an icon listed over here https://heroicons.com/
  const downloadIcon = (merchantId) => {
    const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    icon.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    icon.setAttribute("fill", "none");
    icon.setAttribute("viewBox", "0 0 24 24");
    icon.setAttribute("stroke-width", "2.5");
    icon.setAttribute("stroke", "currentColor");
    icon.classList.add(
      "download-icon",
      "size-6",
      "mr-2",
      "cursor-pointer",
      "w-4",
      "h-4",
    );
    icon.dataset.merchantId = merchantId;

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("stroke-linecap", "round");
    path.setAttribute("stroke-linejoin", "round");
    path.setAttribute("d", "M19.5 8.25L12 15.75 4.5 8.25");

    icon.appendChild(path);
    return icon;
  };

  // analyticsIcon DOM API builds an icon listed over here https://heroicons.com/
  const analyticsIcon = (merchantId) => {
    const icon = document.createElementNS("http://www.w3.org/2000/svg", "svg");
    icon.setAttribute("xmlns", "http://www.w3.org/2000/svg");
    icon.setAttribute("fill", "none");
    icon.setAttribute("viewBox", "0 0 24 24");
    icon.setAttribute("stroke-width", "1.5");
    icon.setAttribute("stroke", "currentColor");
    icon.classList.add(
      "search-icon",
      "size-6",
      "mr-2",
      "cursor-pointer",
      "w-4",
      "h-4",
    );
    icon.dataset.merchantId = merchantId;

    const path = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path.setAttribute("stroke-linecap", "round");
    path.setAttribute("stroke-linejoin", "round");
    path.setAttribute(
      "d",
      "M21 21l-5.197-5.197m0 0A7.5 7.5 0 1 0 5.196 5.196a7.5 7.5 0 0 0 10.607 10.607Z",
    );

    icon.appendChild(path);
    return icon;
  };

  function restoreDownloadIcons() {
    document.querySelectorAll(".download-icon").forEach((iconElement) => {
      const iconMerchantId = iconElement.dataset.merchantId;
      if (iconMerchantId !== currentDownloadMerchantID) {
        iconElement.classList.remove("hidden");
      }
    });
  }
})();
