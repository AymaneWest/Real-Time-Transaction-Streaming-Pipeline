"""
Dashboard — FastAPI Backend + Chart.js Frontend
------------------------------------------------
Serves a real-time analytics dashboard that reads from MongoDB
and exposes REST API endpoints consumed by the Chart.js frontend.
"""

import os
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from pymongo import MongoClient

# ── Config ────────────────────────────────────────────────────────────────────
MONGO_URI        = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB         = os.getenv("MONGO_DB", "transactions_db")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "transactions")

app = FastAPI(title="Streaming Dashboard API")

# ── MongoDB connection ────────────────────────────────────────────────────────
def get_collection():
    for attempt in range(1, 15):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")
            return client[MONGO_DB][MONGO_COLLECTION]
        except Exception as e:
            print(f"[dashboard] Waiting for MongoDB... {attempt}: {e}")
            time.sleep(4)
    raise RuntimeError("MongoDB unavailable")

collection = get_collection()
print("[dashboard] MongoDB connected.")

# ── API routes ────────────────────────────────────────────────────────────────

@app.get("/api/stats")
def stats():
    """High-level KPI numbers."""
    total     = collection.count_documents({})
    success   = collection.count_documents({"status": "success"})
    pending   = collection.count_documents({"status": "pending"})
    failed    = collection.count_documents({"status": "failed"})
    src1      = collection.count_documents({"source": "source1"})
    src2      = collection.count_documents({"source": "source2"})

    pipeline = [{"$group": {"_id": None, "total_amount": {"$sum": "$amount"}}}]
    result   = list(collection.aggregate(pipeline))
    total_amount = round(result[0]["total_amount"], 2) if result else 0.0

    return {
        "total": total,
        "success": success,
        "pending": pending,
        "failed": failed,
        "source1_count": src1,
        "source2_count": src2,
        "total_amount": total_amount,
    }

@app.get("/api/by-category")
def by_category():
    """Transaction count per category."""
    pipeline = [
        {"$group": {"_id": "$category", "count": {"$sum": 1}, "total": {"$sum": "$amount"}}},
        {"$sort": {"count": -1}},
    ]
    return list(collection.aggregate(pipeline))

@app.get("/api/by-source")
def by_source():
    """Transaction volume per source."""
    pipeline = [
        {"$group": {"_id": "$source", "count": {"$sum": 1}, "total": {"$sum": "$amount"}}},
    ]
    return list(collection.aggregate(pipeline))

@app.get("/api/by-status")
def by_status():
    """Status breakdown."""
    pipeline = [
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
    ]
    return list(collection.aggregate(pipeline))

@app.get("/api/recent")
def recent(limit: int = 20):
    """Most recent transactions."""
    docs = list(
        collection.find({}, {"_id": 0})
                  .sort("_id", -1)
                  .limit(limit)
    )
    return docs

@app.get("/api/timeseries")
def timeseries():
    """Transactions bucketed by minute — done in Python to avoid MongoDB date parsing errors."""
    from collections import defaultdict
    from datetime import datetime

    docs = list(collection.find({}, {"_id": 0, "timestamp": 1, "amount": 1}))

    buckets: dict = defaultdict(lambda: {"count": 0, "total": 0.0})

    for doc in docs:
        ts_raw = doc.get("timestamp", "")
        try:
            # Handle both "2026-03-05T14:32:10Z" and "2026-03-05T14:32:10.123Z"
            ts_clean = ts_raw.replace("Z", "+00:00")
            dt = datetime.fromisoformat(ts_clean)
            minute = dt.strftime("%H:%M")
            buckets[minute]["count"] += 1
            buckets[minute]["total"] += float(doc.get("amount", 0))
        except Exception:
            continue  # silently skip malformed timestamps

    result = [
        {"_id": minute, "count": v["count"], "total": round(v["total"], 2)}
        for minute, v in sorted(buckets.items())
    ]
    return result[-30:]  # last 30 minutes

# ── Chart.js Dashboard HTML ───────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def dashboard():
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Real-Time Transaction Pipeline Dashboard</title>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', system-ui, sans-serif;
      background: #0f1117;
      color: #e2e8f0;
      min-height: 100vh;
    }
    header {
      background: linear-gradient(135deg, #1a1f2e 0%, #16213e 100%);
      border-bottom: 1px solid #2d3748;
      padding: 20px 32px;
      display: flex;
      align-items: center;
      gap: 16px;
    }
    header h1 { font-size: 1.5rem; font-weight: 700; color: #fff; }
    header span { font-size: 0.85rem; color: #718096; margin-top: 2px; }
    .dot { width: 10px; height: 10px; background: #48bb78; border-radius: 50%;
           animation: pulse 2s infinite; flex-shrink: 0; }
    @keyframes pulse {
      0%,100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.5; transform: scale(1.3); }
    }

    .kpis {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 16px;
      padding: 24px 32px 0;
    }
    .kpi {
      background: #1a1f2e;
      border: 1px solid #2d3748;
      border-radius: 12px;
      padding: 20px;
      text-align: center;
    }
    .kpi .label { font-size: 0.75rem; color: #718096; text-transform: uppercase; letter-spacing: .05em; }
    .kpi .value { font-size: 2rem; font-weight: 700; margin-top: 8px; }
    .kpi.total  .value { color: #63b3ed; }
    .kpi.success .value { color: #48bb78; }
    .kpi.pending .value { color: #ecc94b; }
    .kpi.failed  .value { color: #fc8181; }
    .kpi.amount  .value { color: #b794f4; font-size: 1.4rem; }
    .kpi.src     .value { color: #76e4f7; }

    .charts {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      padding: 24px 32px;
    }
    .chart-card {
      background: #1a1f2e;
      border: 1px solid #2d3748;
      border-radius: 12px;
      padding: 20px;
    }
    .chart-card.wide { grid-column: 1 / -1; }
    .chart-card h2 { font-size: 0.9rem; color: #a0aec0; margin-bottom: 16px; font-weight: 600; text-transform: uppercase; letter-spacing: .05em; }
    canvas { max-height: 260px; }

    .table-card {
      margin: 0 32px 32px;
      background: #1a1f2e;
      border: 1px solid #2d3748;
      border-radius: 12px;
      overflow: hidden;
    }
    .table-card h2 { font-size: 0.9rem; color: #a0aec0; padding: 16px 20px; font-weight: 600; text-transform: uppercase; letter-spacing: .05em; border-bottom: 1px solid #2d3748; }
    table { width: 100%; border-collapse: collapse; font-size: 0.82rem; }
    th { background: #16213e; color: #718096; padding: 10px 16px; text-align: left; font-weight: 600; }
    td { padding: 10px 16px; border-top: 1px solid #2d3748; }
    tr:hover td { background: #16213e; }
    .badge { padding: 2px 8px; border-radius: 9999px; font-size: 0.75rem; font-weight: 600; }
    .badge.success { background: #1c4532; color: #48bb78; }
    .badge.pending { background: #5f370e; color: #ecc94b; }
    .badge.failed  { background: #63171b; color: #fc8181; }
    .badge.source1 { background: #1a365d; color: #63b3ed; }
    .badge.source2 { background: #3d2a6e; color: #b794f4; }
  </style>
</head>
<body>
  <header>
    <div class="dot"></div>
    <div>
      <h1>Real-Time Transaction Pipeline</h1>
      <span>Kafka → MongoDB → Chart.js &nbsp;|&nbsp; Auto-refresh every 5s</span>
    </div>
  </header>

  <div class="kpis">
    <div class="kpi total">  <div class="label">Total Transactions</div><div class="value" id="kTotal">—</div></div>
    <div class="kpi success"><div class="label">Success</div>           <div class="value" id="kSuccess">—</div></div>
    <div class="kpi pending"><div class="label">Pending</div>           <div class="value" id="kPending">—</div></div>
    <div class="kpi failed"> <div class="label">Failed</div>            <div class="value" id="kFailed">—</div></div>
    <div class="kpi amount"> <div class="label">Total Volume</div>      <div class="value" id="kAmount">—</div></div>
    <div class="kpi src">    <div class="label">Source 1 / Source 2</div><div class="value" id="kSrc">—</div></div>
  </div>

  <div class="charts">
    <div class="chart-card wide">
      <h2>📈 Transactions over Time (last 30 min)</h2>
      <canvas id="tsChart"></canvas>
    </div>
    <div class="chart-card">
      <h2>🏷️ By Category</h2>
      <canvas id="catChart"></canvas>
    </div>
    <div class="chart-card">
      <h2>✅ Status Distribution</h2>
      <canvas id="statusChart"></canvas>
    </div>
  </div>

  <div class="table-card">
    <h2>🕐 Recent Transactions</h2>
    <table>
      <thead>
        <tr>
          <th>ID</th><th>Source</th><th>Merchant</th>
          <th>Category</th><th>Amount</th><th>Status</th><th>Time</th>
        </tr>
      </thead>
      <tbody id="txTable"></tbody>
    </table>
  </div>

  <script>
    const COLORS = ['#63b3ed','#b794f4','#48bb78','#ecc94b','#fc8181','#76e4f7','#f6ad55','#f687b3'];

    function mk(id, type, data, opts = {}) {
      return new Chart(document.getElementById(id), {
        type, data,
        options: {
          responsive: true, maintainAspectRatio: true,
          plugins: { legend: { labels: { color: '#a0aec0', font: { size: 11 } } } },
          scales: type === 'bar' || type === 'line' ? {
            x: { ticks: { color: '#718096' }, grid: { color: '#2d3748' } },
            y: { ticks: { color: '#718096' }, grid: { color: '#2d3748' } },
          } : {},
          ...opts
        }
      });
    }

    const tsChart = mk('tsChart', 'line', { labels: [], datasets: [{
      label: 'Tx / min', data: [], borderColor: '#63b3ed',
      backgroundColor: 'rgba(99,179,237,0.1)', fill: true, tension: 0.4,
    }]});

    const catChart = mk('catChart', 'bar', { labels: [], datasets: [{
      label: 'Count', data: [],
      backgroundColor: COLORS,
    }]}, { indexAxis: 'y' });

    const statusChart = mk('statusChart', 'doughnut', { labels: [], datasets: [{
      data: [], backgroundColor: ['#48bb78','#ecc94b','#fc8181'],
    }]});

    async function safeFetch(url) {
      try {
        const r = await fetch(url);
        if (!r.ok) throw new Error('HTTP ' + r.status);
        return await r.json();
      } catch(e) {
        console.warn('[safeFetch] ' + url + ' failed:', e.message);
        return null;
      }
    }

    async function refresh() {
      // Each fetch is independent — one failure never blocks the others
      const [stats, ts, cat, status, recent] = await Promise.all([
        safeFetch('/api/stats'),
        safeFetch('/api/timeseries'),
        safeFetch('/api/by-category'),
        safeFetch('/api/by-status'),
        safeFetch('/api/recent?limit=15'),
      ]);

      if (stats) {
        document.getElementById('kTotal').textContent   = stats.total.toLocaleString();
        document.getElementById('kSuccess').textContent = stats.success.toLocaleString();
        document.getElementById('kPending').textContent = stats.pending.toLocaleString();
        document.getElementById('kFailed').textContent  = stats.failed.toLocaleString();
        document.getElementById('kAmount').textContent  = '$' + stats.total_amount.toLocaleString();
        document.getElementById('kSrc').textContent     = stats.source1_count + ' / ' + stats.source2_count;
      }

      if (ts && Array.isArray(ts)) {
        tsChart.data.labels = ts.map(d => d._id);
        tsChart.data.datasets[0].data = ts.map(d => d.count);
        tsChart.update();
      }

      if (cat && Array.isArray(cat)) {
        catChart.data.labels = cat.map(d => d._id || 'unknown');
        catChart.data.datasets[0].data = cat.map(d => d.count);
        catChart.update();
      }

      if (status && Array.isArray(status)) {
        statusChart.data.labels = status.map(d => d._id);
        statusChart.data.datasets[0].data = status.map(d => d.count);
        statusChart.update();
      }

      if (recent && Array.isArray(recent)) {
        const tbody = document.getElementById('txTable');
        tbody.innerHTML = recent.map(tx => {
          const amt  = parseFloat(tx.amount);
          const amtStr = isNaN(amt) ? '—' : '$' + amt.toFixed(2) + ' ' + (tx.currency || '');
          const time = tx.timestamp ? new Date(tx.timestamp).toLocaleTimeString() : '—';
          return '<tr>'
            + '<td style="font-family:monospace;color:#718096">' + (tx.transaction_id||'?').slice(0,8) + '…</td>'
            + '<td><span class="badge ' + (tx.source||'') + '">' + (tx.source||'—') + '</span></td>'
            + '<td>' + (tx.merchant||'—') + '</td>'
            + '<td>' + (tx.category||'—') + '</td>'
            + '<td style="color:#b794f4">' + amtStr + '</td>'
            + '<td><span class="badge ' + (tx.status||'') + '">' + (tx.status||'—') + '</span></td>'
            + '<td style="color:#718096">' + time + '</td>'
            + '</tr>';
        }).join('');
      }
    }
    refresh();
    setInterval(refresh, 5000);
  </script>
</body>
</html>
"""

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=3030, reload=False)