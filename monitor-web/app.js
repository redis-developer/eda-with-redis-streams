const POLL_INTERVAL_MS = 1000;

const currency = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
  maximumFractionDigits: 2,
});

const integer = new Intl.NumberFormat("en-US");

const ui = {
  connectionDot: document.getElementById("connection-dot"),
  connectionLabel: document.getElementById("connection-label"),
  generatedAt: document.getElementById("generated-at"),
  streamTransactions: document.getElementById("stream-transactions"),
  metricsLag: document.getElementById("metrics-lag"),
  metricsConsumers: document.getElementById("metrics-consumers"),
  transactionsBody: document.getElementById("transactions-body"),
  alertsList: document.getElementById("alerts-list"),
  categoryBars: document.getElementById("category-bars"),
  regionGrid: document.getElementById("region-grid"),
};

function setConnectionState(mode, label) {
  ui.connectionDot.className = `dot ${mode}`;
  ui.connectionLabel.textContent = label;
}

function formatTimestamp(epochMillis) {
  return new Date(epochMillis).toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function renderTransactions(transactions) {
  if (!transactions.length) {
    ui.transactionsBody.innerHTML = '<tr><td colspan="6" class="empty-row">No transactions yet.</td></tr>';
    return;
  }

  ui.transactionsBody.innerHTML = transactions.map((tx) => `
    <tr>
      <td><span class="${tx.highRisk ? "txn-risk" : "txn-ok"}">${tx.highRisk ? "RISK" : "OK"}</span></td>
      <td class="mono">${tx.shortId}</td>
      <td>${currency.format(tx.amount)}</td>
      <td>${tx.category}</td>
      <td>${tx.region}</td>
      <td><span class="risk-pill ${tx.highRisk ? "high" : ""}">${tx.riskScore}</span></td>
    </tr>
  `).join("");
}

function renderAlerts(alerts) {
  if (!alerts.length) {
    ui.alertsList.innerHTML = '<p class="empty-row">No alert events yet.</p>';
    return;
  }

  ui.alertsList.innerHTML = alerts.map((alert) => `
    <article class="alert-row ${alert.severity}">
      <p class="alert-title">${alert.alertType}</p>
      <p class="alert-meta">
        ${alert.region} · observed ${alert.observedValue} / threshold ${alert.threshold} · ${alert.windowSeconds}s
      </p>
      <p class="alert-message">${alert.message}</p>
    </article>
  `).join("");
}

function renderCategoryBars(categories) {
  if (!categories.length) {
    ui.categoryBars.innerHTML = '<p class="empty-row">No category metrics yet.</p>';
    return;
  }

  const maxValue = Math.max(...categories.map((entry) => entry.volume), 1);
  ui.categoryBars.innerHTML = categories.map((entry) => {
    const width = `${(entry.volume / maxValue) * 100}%`;
    return `
      <div class="bar-row">
        <div class="bar-meta">
          <span>${entry.category}</span>
          <strong>${currency.format(entry.volume)}</strong>
        </div>
        <div class="bar-track">
          <div class="bar-fill" style="width:${width}"></div>
        </div>
      </div>
    `;
  }).join("");
}

function renderRegionGrid(regionCounts) {
  const entries = Object.entries(regionCounts ?? {});
  if (!entries.length) {
    ui.regionGrid.innerHTML = '<p class="empty-row">No region metrics yet.</p>';
    return;
  }

  ui.regionGrid.innerHTML = entries.map(([region, value]) => `
    <article class="region-card">
      <p class="region-name">${region}</p>
      <p class="region-value">${integer.format(value)}</p>
    </article>
  `).join("");
}

function renderSnapshot(snapshot) {
  ui.generatedAt.textContent = `Updated ${formatTimestamp(Date.parse(snapshot.generatedAt))}`;
  ui.streamTransactions.textContent = integer.format(snapshot.streams.transactions);
  ui.metricsLag.textContent = integer.format(snapshot.dashboard.metricsLag);
  ui.metricsConsumers.textContent = integer.format(snapshot.dashboard.metricsConsumers);

  renderTransactions(snapshot.transactions);
  renderAlerts(snapshot.alerts);
  renderCategoryBars(snapshot.metrics.categories);
  renderRegionGrid(snapshot.metrics.regionCounts);
}

async function poll() {
  try {
    const response = await fetch("/api/monitor", { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const snapshot = await response.json();
    renderSnapshot(snapshot);
    setConnectionState("online", "Live");
  } catch (error) {
    setConnectionState("error", "Disconnected");
    console.error("Unable to load monitor snapshot", error);
  }
}

poll();
setInterval(poll, POLL_INTERVAL_MS);
