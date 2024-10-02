document.addEventListener("DOMContentLoaded", () => {
  const telemetry = document.getElementById("telemetry-data");

  const worker = new Worker("service-worker.js"); // should technically check Worker API exists
  worker.addEventListener("message", (event) => {
    const { action, buffer, message } = event.data;
    switch (action) {
      case "db-ready":
        initDbFromWorker(buffer);
        loadMerchantButton.classList.remove(
          "opacity-50",
          "pointer-events-none",
        );
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
  loadMerchantButton.addEventListener("click", () => {
    loadMerchantButton.classList.add("opacity-50", "pointer-events-none");
    worker.postMessage({ merchantID: "d773d571-7fce-4aa6-ad04-05e37a93fb26" });
  });

  setInterval(pollTelemetry, 1000);
});

const initDbFromWorker = async (buffer) => {
  const SQL = await window.initSqlJs({
    locateFile: (file) => `https://sql.js.org/dist/${file}`,
  });
  db = new SQL.Database(new Uint8Array(buffer));
  window.db = db;
  console.info("database loaded from worker and ready to use - see window.db");
};
