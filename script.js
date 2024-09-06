document.addEventListener("DOMContentLoaded", function () {
  let seconds = 0;
  const timeElement = document.getElementById("time");

  setInterval(() => {
    seconds++;
    timeElement.textContent = `${seconds}s`;
  }, 1000);
});
