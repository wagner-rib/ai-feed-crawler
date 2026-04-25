/* AI Feed Aggregator — frontend JS */

// Auto-submit search form on clear
document.querySelectorAll('input[name="q"]').forEach(el => {
  el.addEventListener("input", () => {
    if (el.value === "") el.closest("form").submit();
  });
});

// Refresh countdown (2h = 7200s)
function startRefreshCountdown() {
  const el = document.getElementById("refresh-countdown");
  if (!el) return;
  let seconds = parseInt(el.dataset.seconds || "7200", 10);
  setInterval(() => {
    seconds = Math.max(0, seconds - 1);
    const h = Math.floor(seconds / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = seconds % 60;
    el.textContent = `${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}:${String(s).padStart(2,"0")}`;
  }, 1000);
}
startRefreshCountdown();

// Manual refresh button
const refreshBtn = document.getElementById("refresh-btn");
if (refreshBtn) {
  refreshBtn.addEventListener("click", async () => {
    refreshBtn.disabled = true;
    refreshBtn.innerHTML = '<span class="spinner"></span> Refreshing…';
    try {
      const res = await fetch("/api/refresh");
      const data = await res.json();
      const n = data.new_articles || 0;
      refreshBtn.innerHTML = `✓ +${n} new articles`;
      setTimeout(() => location.reload(), 1500);
    } catch {
      refreshBtn.innerHTML = "Refresh failed";
      refreshBtn.disabled = false;
    }
  });
}

// Lazy-load images
if ("IntersectionObserver" in window) {
  const obs = new IntersectionObserver((entries) => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        const img = e.target;
        if (img.dataset.src) {
          img.src = img.dataset.src;
          img.removeAttribute("data-src");
        }
        obs.unobserve(img);
      }
    });
  }, { rootMargin: "200px" });
  document.querySelectorAll("img[data-src]").forEach(img => obs.observe(img));
}

// Animate numbers
function animateNum(el) {
  const target = parseInt(el.dataset.target, 10);
  if (isNaN(target)) return;
  let current = 0;
  const step = Math.ceil(target / 60);
  const timer = setInterval(() => {
    current = Math.min(current + step, target);
    el.textContent = current.toLocaleString();
    if (current >= target) clearInterval(timer);
  }, 20);
}
document.querySelectorAll(".count-up").forEach(animateNum);
