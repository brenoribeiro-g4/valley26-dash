"""
Vercel Serverless Function — /api/data
Queries Databricks and returns fresh dashboard JSON.

Query params:
  ?start=2026-07-07&end=2026-07-31  (defaults to full lote period)

Env vars required (set in Vercel dashboard):
  DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
"""

import json
import os
import time
from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import urllib.request
import urllib.error

# ============================================================
# CONFIG
# ============================================================
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-8acefaf9-a170.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "bbae754ea44f67e0")

EDITION = "g4valley-1026"
DEFAULT_START = "2026-07-07"
DEFAULT_END = "2026-07-31"


def run_query(sql):
    """Execute SQL via Databricks Statement API."""
    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
    payload = json.dumps({
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "45s"
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/json"
    })
    try:
        with urllib.request.urlopen(req, timeout=50) as resp:
            data = json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return None

    state = data.get("status", {}).get("state", "")
    if state == "SUCCEEDED":
        return data["result"]["data_array"]

    # Poll if pending
    if state in ("PENDING", "RUNNING"):
        stmt_id = data.get("statement_id")
        for _ in range(24):
            time.sleep(5)
            poll_url = f"{url}/{stmt_id}"
            poll_req = urllib.request.Request(poll_url, headers={"Authorization": f"Bearer {DATABRICKS_TOKEN}"})
            with urllib.request.urlopen(poll_req, timeout=10) as resp:
                poll = json.loads(resp.read())
            if poll.get("status", {}).get("state") == "SUCCEEDED":
                return poll["result"]["data_array"]
            if poll.get("status", {}).get("state") == "FAILED":
                return None
    return None


CHANNEL_CASE = """
    CASE
        WHEN utm_source = 'facebook' AND LOWER(utm_campaign) LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%' THEN 'meta_ads'
        WHEN utm_source = 'facebook' THEN 'facebook_other'
        WHEN utm_source = 'google' AND LOWER(utm_campaign) LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%' THEN 'google_ads'
        WHEN utm_source = 'google' THEN 'google_other'
        WHEN utm_source IN ('instagram', 'ig') AND LOWER(COALESCE(utm_medium,'')) IN ('tg', 'tg_social', 'tallis', 'thallis') THEN 'tg_social'
        WHEN utm_source IN ('instagram', 'ig') AND LOWER(COALESCE(utm_medium,'')) IN ('alfredo', 'alf') THEN 'alfredo'
        WHEN utm_source IN ('instagram', 'ig') AND LOWER(COALESCE(utm_medium,'')) IN ('nardon', 'nard') THEN 'nardon'
        WHEN utm_source IN ('instagram', 'ig') THEN 'g4_social'
        ELSE COALESCE(utm_source, 'nao-definido')
    END
"""

MKT_FILTER = """(
    LOWER(utm_campaign) LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'
    OR LOWER(utm_campaign) LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%'
)"""


def build_data(start, end):
    """Build dashboard JSON with a SINGLE combined query for speed."""
    from datetime import datetime, date

    # ONE combined query: by_day_canal gives us everything we need
    # (totals, by_day, by_canal, by_day_canal all derivable from this)
    rows = run_query(f"""
        SELECT
            CAST(dt_event AS STRING) as dia,
            {CHANNEL_CASE} as canal,
            COUNT(*) as vendas,
            ROUND(SUM(vl_venda), 2) as fat
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}'
          AND dt_event >= '{start}' AND dt_event <= '{end}'
        GROUP BY 1, 2
        ORDER BY 1, 4 DESC
    """)

    if not rows:
        return {"error": "Query failed", "total_vendas": 0}

    # Derive all aggregations from one result set
    total_vendas = 0
    total_fat = 0
    by_day = {}
    by_canal = {}
    by_day_canal = {}

    for r in rows:
        dia, canal, vendas, fat = r[0], r[1], int(r[2]), float(r[3] or 0)
        total_vendas += vendas
        total_fat += fat

        if dia not in by_day:
            by_day[dia] = {"fat": 0, "vendas": 0}
        by_day[dia]["fat"] += fat
        by_day[dia]["vendas"] += vendas

        if canal not in by_canal:
            by_canal[canal] = {"fat": 0, "vendas": 0}
        by_canal[canal]["fat"] += fat
        by_canal[canal]["vendas"] += vendas

        if dia not in by_day_canal:
            by_day_canal[dia] = {}
        by_day_canal[dia][canal] = {"fat": fat, "vendas": vendas}

    ticket_medio = round(total_fat / total_vendas, 2) if total_vendas > 0 else 0

    # Marketing (second query)
    mkt_rows = run_query(f"""
        SELECT COALESCE(utm_source,'nd'), event, ROUND(SUM(event_value),2)
        FROM production.gold.marketing_fct
        WHERE {MKT_FILTER} AND event_at >= '{start}' AND event_at <= '{end}'
        GROUP BY 1, 2
    """)
    mkt_by_canal = {}
    total_invest = 0
    for r in (mkt_rows or []):
        canal, event, val = r[0], r[1], float(r[2] or 0)
        if canal not in mkt_by_canal:
            mkt_by_canal[canal] = {}
        mkt_by_canal[canal][event] = val
        if event == "investimento":
            total_invest += val

    # Invest by day (third query)
    inv_rows = run_query(f"""
        SELECT CAST(event_at AS STRING), ROUND(SUM(event_value),2)
        FROM production.gold.marketing_fct
        WHERE {MKT_FILTER} AND event_at >= '{start}' AND event_at <= '{end}' AND event = 'investimento'
        GROUP BY 1
    """)
    for r in (inv_rows or []):
        dia, inv = r[0], float(r[1] or 0)
        if dia in by_day:
            by_day[dia]["invest"] = round(inv, 2)

    # Leads (fourth query)
    leads_rows = run_query(f"""
        SELECT COUNT(*) FROM g4_eventos_lancamentos.vw_mart_eventos_leads_pre_inscricao
        WHERE cl_edicao_evento_pre_inscricao = '{EDITION}'
    """)
    total_leads = int(leads_rows[0][0]) if leads_rows else 0

    # Perf windows
    today_str = date.today().isoformat()
    yesterday_str = (date.today().replace(day=date.today().day - 1)).isoformat() if date.today().day > 1 else today_str
    today_data = by_day.get(today_str, {"fat": 0, "vendas": 0})
    yesterday_data = by_day.get(yesterday_str, {"fat": 0, "vendas": 0})
    ovd1_fat = sum(d["fat"] for k, d in by_day.items() if k < today_str)
    ovd1_vendas = sum(d["vendas"] for k, d in by_day.items() if k < today_str)
    invest_hoje = by_day.get(today_str, {}).get("invest", 0)
    invest_d1 = by_day.get(yesterday_str, {}).get("invest", 0)
    invest_ovd1 = sum(d.get("invest", 0) for k, d in by_day.items() if k < today_str)

    def calc_perf(fat, vendas, invest):
        return {
            "invest": round(invest, 2), "vendas": vendas, "fat": round(fat, 2),
            "ticket_medio": round(fat / vendas, 2) if vendas > 0 else 0,
            "cpa": round(invest / vendas, 2) if vendas > 0 else 0,
            "roas": round(fat / invest, 2) if invest > 0 else 0
        }

    perf = {
        "geral": calc_perf(total_fat, total_vendas, total_invest),
        "d1": calc_perf(yesterday_data["fat"], yesterday_data["vendas"], invest_d1),
        "hoje": calc_perf(today_data["fat"], today_data["vendas"], invest_hoje),
        "ovd1": calc_perf(ovd1_fat, ovd1_vendas, invest_ovd1)
    }

    meta_invest = mkt_by_canal.get("facebook", {}).get("investimento", 0)
    google_invest = mkt_by_canal.get("google", {}).get("investimento", 0)
    perf_canal = {
        "meta_ads": calc_perf(by_canal.get("meta_ads", {}).get("fat", 0), by_canal.get("meta_ads", {}).get("vendas", 0), meta_invest),
        "google": calc_perf(by_canal.get("google_ads", {}).get("fat", 0), by_canal.get("google_ads", {}).get("vendas", 0), google_invest),
        "youtube": calc_perf(0, 0, 0)
    }

    ads_report = []  # From inline/fetch script

    return {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "edition": EDITION,
        "lote_start": start,
        "lote_end": end,
        "total_fat": total_fat,
        "total_vendas": total_vendas,
        "total_invest": round(total_invest, 2),
        "ticket_medio": ticket_medio,
        "total_leads": total_leads,
        "by_day": by_day,
        "by_canal": by_canal,
        "by_day_canal": by_day_canal,
        "by_hour": by_hour,
        "perf": perf,
        "perf_canal": perf_canal,
        "mkt_by_canal": {k: {kk: round(vv, 2) for kk, vv in v.items()} for k, v in mkt_by_canal.items()},
        "ads_report": ads_report,
        "raw_rows": []
    }


class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not DATABRICKS_TOKEN:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "DATABRICKS_TOKEN not configured"}).encode())
            return

        # Parse query params
        query = parse_qs(urlparse(self.path).query)
        start = query.get("start", [DEFAULT_START])[0]
        end = query.get("end", [DEFAULT_END])[0]

        try:
            result = build_data(start, end)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "s-maxage=60, stale-while-revalidate=300")
            self.end_headers()
            self.wfile.write(json.dumps(result, ensure_ascii=False).encode())
        except Exception as e:
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": str(e)}).encode())

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
