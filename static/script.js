document.addEventListener("DOMContentLoaded", () => {
  const diagnostics = document.getElementById("diagnostic-data");
  const pollDiagnostics = async () => {
    const response = await fetch("/diagnostics");
    if (response.ok) {
      const data = await response.json();
      diagnostics.innerHTML = "";

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

        diagnostics.appendChild(p);
      }
    } else {
      console.error("error fetching diagnostics data");
    }
  };

  setInterval(pollDiagnostics, 1000);
});
