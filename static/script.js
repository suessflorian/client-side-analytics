document.addEventListener("DOMContentLoaded", () => {
  const telemetry = document.getElementById("telemetry-data");

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
    console.log("hello");
    generateButton.classList.add("opacity-50", "pointer-events-none");
    try {
      const response = await fetch("/generate", {
        method: "POST",
      });

      if (response.ok) {
        console.log("Post request successful");
      } else {
        console.error("Post request failed");
      }
    } catch (error) {
      console.error("Error sending post request:", error);
    } finally {
      generateButton.classList.remove("opacity-50", "pointer-events-none");
    }
  });

  setInterval(pollTelemetry, 1000);
});
