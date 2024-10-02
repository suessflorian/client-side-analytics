let currentDownloadMerchantID = null;

document.addEventListener("DOMContentLoaded", () => {
  const telemetry = document.getElementById("telemetry-data");
  const merchantList = document.getElementById("merchant-list");
  const generateButton = document.getElementById("generate-button");

  const worker = new Worker("service-worker.js"); // should technically check Worker API exists

  worker.addEventListener("message", (event) => {
    const { action, buffer, message, merchantID } = event.data;
    switch (action) {
      case "db-ready":
        initDbFromWorker(buffer, merchantID);
        restoreDownloadIcons();
        break;

      case "error":
        console.error(message);
        break;
    }
  });

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
    }
  };

  generateButton.addEventListener("click", async () => {
    generateButton.classList.add("opacity-50", "pointer-events-none");
    try {
      const response = await fetch("/generate", {
        method: "POST",
      });
      if (response.ok) {
        const data = await response.json();
        console.log(
          `${data.Transactions.toLocaleString()} transactions, ${data.Lines.toLocaleString()} transaction lines - over ${data.Merchants.length.toLocaleString()} merchants`,
        );
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
          icon.addEventListener("click", () => {
            const merchantID = merchant.ID.toString();
            currentDownloadMerchantID = merchantID;
            icon.classList.add("hidden");
            restoreDownloadIcons();
            worker.postMessage({ action: "download", merchantID: merchant.ID });
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

  setInterval(pollTelemetry, 1000);
});

function restoreDownloadIcons() {
  document.querySelectorAll(".download-icon").forEach((iconElement) => {
    const iconMerchantId = iconElement.dataset.merchantId;
    if (iconMerchantId !== currentDownloadMerchantID) {
      iconElement.classList.remove("hidden");
    }
  });
}

const runAnalytics = async (merchantID) => {
  if (currentDownloadMerchantID === merchantID) {
    try {
      const data = window.db.exec(`SELECT
        p.id AS product_id,
        p.name AS product_name,
        SUM(p.price_cents * tl.quantity) AS total_revenue
      FROM main_products p
      JOIN main_transaction_lines tl ON p.id = tl.product_id
      GROUP BY p.id, p.name
      ORDER BY total_revenue DESC, product_name ASC
      LIMIT 5;`);
      console.log("sqlite response:", data);
    } catch (error) {
      console.error("error executing query:", error);
    }
  } else {
    try {
      const response = await fetch(`/analytics/${merchantID}`);
      const data = await response.json();
      console.log("server response:", data);
    } catch (error) {
      console.error("error fetching analytics:", error);
    }
  }
};

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

const initDbFromWorker = async (buffer, merchantID) => {
  const SQL = await window.initSqlJs({
    locateFile: (file) => `https://sql.js.org/dist/${file}`,
  });
  db = new SQL.Database(new Uint8Array(buffer));
  window.db = db;
  console.info(`database loaded for ${merchantID} - see window.db`);
  const names = db.exec(
    `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';`,
  );
  names[0].values.forEach((table) => {
    const name = table[0];
    const rowCount = db.exec(`SELECT COUNT(*) AS row_count FROM ${name};`);
    const count = rowCount[0].values[0][0];
    console.log(`${name} with ${count} rows`);
  });
};
