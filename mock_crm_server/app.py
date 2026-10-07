"""
mock_crm_server/app.py
-----------------------
Mock CRM REST API Server (Flask)
---------------------------------
Simulates a real CRM system (like HubSpot / Salesforce) that:
  - Accepts individual and bulk customer segment pushes
  - Stores received data in-memory (+ SQLite for persistence)
  - Exposes a dashboard endpoint to view received data
  - Provides health check endpoint

Routes:
  POST /api/crm/customers          — Push single customer
  POST /api/crm/customers/bulk     — Push bulk customers
  GET  /api/crm/customers          — List all customers in CRM
  GET  /api/crm/customers/<id>     — Get one customer
  GET  /api/crm/dashboard          — Segment summary dashboard
  GET  /api/health                 — Health check
  GET  /                           — HTML dashboard UI
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "crm_store.db"


# ── Database Setup ────────────────────────────────────────────────────────────

def get_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS crm_customers (
            customer_id             TEXT PRIMARY KEY,
            name                    TEXT,
            email                   TEXT,
            region                  TEXT,
            segment                 TEXT,
            rfm_score               INTEGER,
            total_spend             REAL,
            purchase_frequency      INTEGER,
            days_since_last_purchase INTEGER,
            recommended_action      TEXT,
            warehouse_run_id        INTEGER,
            synced_at               TEXT,
            received_at             TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


init_db()


# ── Helpers ───────────────────────────────────────────────────────────────────

def upsert_customer(conn, data: dict):
    conn.execute("""
        INSERT INTO crm_customers
            (customer_id, name, email, region, segment, rfm_score,
             total_spend, purchase_frequency, days_since_last_purchase,
             recommended_action, warehouse_run_id, synced_at, received_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(customer_id) DO UPDATE SET
            name=excluded.name,
            email=excluded.email,
            region=excluded.region,
            segment=excluded.segment,
            rfm_score=excluded.rfm_score,
            total_spend=excluded.total_spend,
            purchase_frequency=excluded.purchase_frequency,
            days_since_last_purchase=excluded.days_since_last_purchase,
            recommended_action=excluded.recommended_action,
            warehouse_run_id=excluded.warehouse_run_id,
            synced_at=excluded.synced_at,
            received_at=datetime('now')
    """, (
        data.get("customer_id"), data.get("name"), data.get("email"),
        data.get("region"), data.get("segment"), data.get("rfm_score"),
        data.get("total_spend"), data.get("purchase_frequency"),
        data.get("days_since_last_purchase"), data.get("recommended_action"),
        data.get("warehouse_run_id"), data.get("synced_at"),
        datetime.utcnow().isoformat()
    ))


# ── API Routes ────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "Mock CRM API", "timestamp": datetime.utcnow().isoformat()})


@app.route("/api/crm/customers", methods=["POST"])
def push_customer():
    data = request.get_json()
    if not data or "customer_id" not in data:
        return jsonify({"error": "Missing customer_id"}), 400

    try:
        conn = get_db()
        upsert_customer(conn, data)
        conn.commit()
        conn.close()
        return jsonify({
            "message": "Customer synced to CRM",
            "customer_id": data["customer_id"],
            "segment": data.get("segment"),
        }), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/crm/customers/bulk", methods=["POST"])
def bulk_push():
    body = request.get_json()
    if not body or "customers" not in body:
        return jsonify({"error": "Missing 'customers' key"}), 400

    customers = body["customers"]
    success, failed = 0, 0
    errors = []

    try:
        conn = get_db()
        for cust in customers:
            try:
                upsert_customer(conn, cust)
                success += 1
            except Exception as e:
                failed += 1
                errors.append({"customer_id": cust.get("customer_id"), "error": str(e)})
        conn.commit()
        conn.close()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({
        "message": "Bulk sync complete",
        "total": len(customers),
        "success_count": success,
        "failed_count": failed,
        "errors": errors,
    }), 201


@app.route("/api/crm/customers", methods=["GET"])
def list_customers():
    conn = get_db()
    rows = conn.execute("SELECT * FROM crm_customers ORDER BY rfm_score DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route("/api/crm/customers/<customer_id>", methods=["GET"])
def get_customer(customer_id):
    conn = get_db()
    row = conn.execute("SELECT * FROM crm_customers WHERE customer_id=?", (customer_id,)).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Customer not found"}), 404
    return jsonify(dict(row))


@app.route("/api/crm/dashboard", methods=["GET"])
def dashboard_api():
    conn = get_db()
    total = conn.execute("SELECT COUNT(*) FROM crm_customers").fetchone()[0]
    segs = conn.execute(
        "SELECT segment, COUNT(*) as count FROM crm_customers GROUP BY segment"
    ).fetchall()
    top = conn.execute(
        "SELECT customer_id, name, segment, rfm_score, recommended_action FROM crm_customers ORDER BY rfm_score DESC LIMIT 5"
    ).fetchall()
    conn.close()
    return jsonify({
        "total_customers": total,
        "segments": {r["segment"]: r["count"] for r in segs},
        "top_customers": [dict(r) for r in top],
    })


# ── HTML Dashboard ────────────────────────────────────────────────────────────

DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Mock CRM Dashboard</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: 'Segoe UI', sans-serif;
      background: #0f172a;
      color: #e2e8f0;
      min-height: 100vh;
      padding: 2rem;
    }
    h1 { font-size: 2rem; color: #38bdf8; margin-bottom: 0.5rem; }
    .subtitle { color: #64748b; margin-bottom: 2rem; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
    .card {
      background: #1e293b;
      border: 1px solid #334155;
      border-radius: 12px;
      padding: 1.5rem;
      text-align: center;
    }
    .card .label { font-size: 0.8rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 1px; }
    .card .value { font-size: 2.5rem; font-weight: 700; margin-top: 0.5rem; }
    .vip { color: #f59e0b; }
    .loyal { color: #34d399; }
    .regular { color: #60a5fa; }
    .risk { color: #f87171; }
    .total { color: #a78bfa; }
    table { width: 100%; border-collapse: collapse; background: #1e293b; border-radius: 12px; overflow: hidden; }
    th { background: #334155; padding: 0.75rem 1rem; text-align: left; font-size: 0.8rem; text-transform: uppercase; color: #94a3b8; }
    td { padding: 0.75rem 1rem; border-top: 1px solid #334155; font-size: 0.9rem; }
    tr:hover td { background: #263347; }
    .badge {
      display: inline-block;
      padding: 2px 10px;
      border-radius: 999px;
      font-size: 0.75rem;
      font-weight: 600;
    }
    .badge-VIP { background: rgba(245,158,11,0.2); color: #f59e0b; }
    .badge-Loyal { background: rgba(52,211,153,0.2); color: #34d399; }
    .badge-Regular { background: rgba(96,165,250,0.2); color: #60a5fa; }
    .badge-At { background: rgba(248,113,113,0.2); color: #f87171; }
    h2 { color: #94a3b8; font-size: 1rem; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 1rem; }
    .section { margin-bottom: 2rem; }
    .refresh-btn {
      background: #38bdf8; color: #0f172a; border: none;
      padding: 0.5rem 1.5rem; border-radius: 8px; cursor: pointer;
      font-weight: 600; margin-bottom: 2rem;
    }
    .refresh-btn:hover { background: #7dd3fc; }
  </style>
</head>
<body>
  <h1>🎯 Mock CRM Dashboard</h1>
  <p class="subtitle">Reverse ETL Pipeline — Customer Segments Received</p>
  <button class="refresh-btn" onclick="location.reload()">↻ Refresh</button>
  <div class="cards" id="cards">Loading...</div>
  <div class="section">
    <h2>📋 All Customers</h2>
    <table id="table">
      <thead><tr>
        <th>ID</th><th>Name</th><th>Email</th><th>Region</th>
        <th>Segment</th><th>RFM Score</th><th>Total Spend</th>
        <th>Recommended Action</th><th>Received At</th>
      </tr></thead>
      <tbody id="tbody">Loading...</tbody>
    </table>
  </div>
<script>
  async function loadDashboard() {
    const dash = await fetch('/api/crm/dashboard').then(r => r.json());
    const segs = dash.segments || {};
    document.getElementById('cards').innerHTML = `
      <div class="card"><div class="label">Total Customers</div><div class="value total">${dash.total_customers}</div></div>
      <div class="card"><div class="label">VIP</div><div class="value vip">${segs['VIP'] || 0}</div></div>
      <div class="card"><div class="label">Loyal</div><div class="value loyal">${segs['Loyal'] || 0}</div></div>
      <div class="card"><div class="label">Regular</div><div class="value regular">${segs['Regular'] || 0}</div></div>
      <div class="card"><div class="label">At Risk</div><div class="value risk">${segs['At Risk'] || 0}</div></div>
    `;
  }
  async function loadTable() {
    const rows = await fetch('/api/crm/customers').then(r => r.json());
    const badgeClass = (seg) => {
      if (seg === 'VIP') return 'badge-VIP';
      if (seg === 'Loyal') return 'badge-Loyal';
      if (seg === 'Regular') return 'badge-Regular';
      return 'badge-At';
    };
    document.getElementById('tbody').innerHTML = rows.map(r => `
      <tr>
        <td>${r.customer_id}</td>
        <td>${r.name}</td>
        <td>${r.email}</td>
        <td>${r.region}</td>
        <td><span class="badge ${badgeClass(r.segment)}">${r.segment}</span></td>
        <td>${r.rfm_score}</td>
        <td>₹${(r.total_spend||0).toLocaleString()}</td>
        <td>${r.recommended_action}</td>
        <td>${r.received_at}</td>
      </tr>
    `).join('');
  }
  loadDashboard();
  loadTable();
</script>
</body>
</html>
"""


@app.route("/", methods=["GET"])
def dashboard_ui():
    return render_template_string(DASHBOARD_HTML)


if __name__ == "__main__":
    print("=" * 60)
    print("  Mock CRM API Server starting on http://127.0.0.1:5050")
    print("  Dashboard: http://127.0.0.1:5050/")
    print("=" * 60)
    app.run(host="127.0.0.1", port=5050, debug=False)
