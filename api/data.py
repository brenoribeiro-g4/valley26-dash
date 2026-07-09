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
    """Build the full dashboard JSON payload."""
    from datetime import datetime, date

    # 1. Vendas totais
    rows = run_query(f"""
        SELECT COUNT(*), ROUND(SUM(vl_venda),2), ROUND(AVG(vl_venda),2)
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}' AND dt_event >= '{start}' AND dt_event <= '{end}'
    """)
    if not rows:
        return {"error": "Query failed", "total_vendas": 0}

    total_vendas = int(rows[0][0])
    total_fat = float(rows[0][1] or 0)
    ticket_medio = float(rows[0][2] or 0)

    # 2. By day
    rows = run_query(f"""
        SELECT CAST(dt_event AS STRING), COUNT(*), ROUND(SUM(vl_venda),2)
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}' AND dt_event >= '{start}' AND dt_event <= '{end}'
        GROUP BY 1 ORDER BY 1
    """)
    by_day = {}
    for r in (rows or []):
        by_day[r[0]] = {"fat": float(r[2] or 0), "vendas": int(r[1])}

    # 3. By canal
    rows = run_query(f"""
        SELECT {CHANNEL_CASE} as canal, COUNT(*), ROUND(SUM(vl_venda),2)
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}' AND dt_event >= '{start}' AND dt_event <= '{end}'
        GROUP BY 1 ORDER BY 3 DESC
    """)
    by_canal = {}
    for r in (rows or []):
        by_canal[r[0]] = {"fat": float(r[2] or 0), "vendas": int(r[1])}

    # 4. By day x canal
    rows = run_query(f"""
        SELECT CAST(dt_event AS STRING), {CHANNEL_CASE} as canal, COUNT(*), ROUND(SUM(vl_venda),2)
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}' AND dt_event >= '{start}' AND dt_event <= '{end}'
        GROUP BY 1, 2 ORDER BY 1, 4 DESC
    """)
    by_day_canal = {}
    for r in (rows or []):
        dia, canal = r[0], r[1]
        if dia not in by_day_canal:
            by_day_canal[dia] = {}
        by_day_canal[dia][canal] = {"fat": float(r[3] or 0), "vendas": int(r[2])}

    # 5. By hour
    rows = run_query(f"""
        SELECT HOUR(ts_event), {CHANNEL_CASE} as canal, COUNT(*), ROUND(SUM(vl_venda),2)
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}' AND dt_event >= '{start}' AND dt_event <= '{end}'
        GROUP BY 1, 2 ORDER BY 1, 4 DESC
    """)
    by_hour = {}
    for r in (rows or []):
        h = str(int(r[0])).zfill(2)
        canal, vendas, fat = r[1], int(r[2]), float(r[3] or 0)
        if h not in by_hour:
            by_hour[h] = {"fat": 0, "vendas": 0, "by_canal": {}}
        by_hour[h]["fat"] += fat
        by_hour[h]["vendas"] += vendas
        by_hour[h]["by_canal"][canal] = fat

    # 6. Marketing
    rows = run_query(f"""
        SELECT COALESCE(utm_source,'nd'), event, ROUND(SUM(event_value),2)
        FROM production.gold.marketing_fct
        WHERE {MKT_FILTER} AND event_at >= '{start}' AND event_at <= '{end}'
        GROUP BY 1, 2 ORDER BY 1, 2
    """)
    mkt_by_canal = {}
    total_invest = 0
    for r in (rows or []):
        canal, event, val = r[0], r[1], float(r[2] or 0)
        if canal not in mkt_by_canal:
            mkt_by_canal[canal] = {}
        mkt_by_canal[canal][event] = val
        if event == "investimento":
            total_invest += val

    # 7. Invest by day
    rows = run_query(f"""
        SELECT CAST(event_at AS STRING), ROUND(SUM(event_value),2)
        FROM production.gold.marketing_fct
        WHERE {MKT_FILTER} AND event_at >= '{start}' AND event_at <= '{end}' AND event = 'investimento'
        GROUP BY 1 ORDER BY 1
    """)
    for r in (rows or []):
        dia, inv = r[0], float(r[1] or 0)
        if dia in by_day:
            by_day[dia]["invest"] = round(inv, 2)

    # 8. Perf windows
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

    # 9. Perf canal
    meta_invest = mkt_by_canal.get("facebook", {}).get("investimento", 0)
    google_invest = mkt_by_canal.get("google", {}).get("investimento", 0)
    perf_canal = {
        "meta_ads": calc_perf(by_canal.get("meta_ads", {}).get("fat", 0), by_canal.get("meta_ads", {}).get("vendas", 0), meta_invest),
        "google": calc_perf(by_canal.get("google_ads", {}).get("fat", 0), by_canal.get("google_ads", {}).get("vendas", 0), google_invest),
        "youtube": calc_perf(0, 0, 0)
    }

    # 10. Leads
    rows = run_query(f"""
        SELECT COUNT(*) FROM g4_eventos_lancamentos.vw_mart_eventos_leads_pre_inscricao
        WHERE cl_edicao_evento_pre_inscricao = '{EDITION}'
    """)
    total_leads = int(rows[0][0]) if rows else 0

    # 11. Ads Report (ad-level metrics + sales attribution)
    # Simplified query to avoid timeout — platform detection done in JS
    ads_report = []
    ads_rows = run_query(f"""
    WITH ads_metrics AS (
        SELECT
            a.ad_name,
            LEFT(m.utm_campaign, 100) as campaign,
            m.adset_id,
            m.ad_id,
            ROUND(SUM(CASE WHEN m.event='investimento' THEN m.event_value ELSE 0 END), 2) as invest,
            ROUND(SUM(CASE WHEN m.event='clicks' THEN m.event_value ELSE 0 END), 0) as clicks,
            ROUND(SUM(CASE WHEN m.event='impressoes' THEN m.event_value ELSE 0 END), 0) as impressoes
        FROM production.gold.marketing_fct m
        LEFT JOIN production.gold.ads_details a ON m.ad_id = a.ad_id
        WHERE (LOWER(m.utm_campaign) LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'
               OR LOWER(m.utm_campaign) LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%')
          AND m.event_at >= '{start}'
          AND m.event_at <= '{end}'
        GROUP BY 1, 2, 3, 4
    ),
    vendas AS (
        SELECT
            LOWER(utm_content) as ad_lower,
            MAX(utm_term) as adset_name_from_deal,
            COUNT(*) as vendas,
            ROUND(SUM(vl_venda), 2) as fat
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}'
          AND dt_event >= '{start}'
          AND dt_event <= '{end}'
          AND (utm_source = 'facebook' OR utm_source = 'google')
        GROUP BY 1
    )
    SELECT
        am.ad_name, am.campaign, am.adset_id, am.ad_id,
        COALESCE(v.adset_name_from_deal, am.adset_id) as adset_name,
        am.invest, am.clicks, am.impressoes,
        ROUND(am.clicks / NULLIF(am.impressoes, 0) * 100, 2) as ctr,
        COALESCE(v.vendas, 0) as vendas,
        COALESCE(v.fat, 0) as fat,
        CASE WHEN COALESCE(v.vendas,0) > 0 THEN ROUND(am.invest / v.vendas, 2) ELSE 0 END as cpa,
        CASE WHEN am.invest > 0 THEN ROUND(COALESCE(v.fat,0) / am.invest, 2) ELSE 0 END as roas
    FROM ads_metrics am
    LEFT JOIN vendas v ON v.ad_lower = LOWER(am.ad_name)
    WHERE am.invest > 0
    ORDER BY am.invest DESC
    LIMIT 100
    """)
    for r in (ads_rows or []):
        ads_report.append({
            "ad_name": r[0] or "",
            "campaign": r[1] or "",
            "adset_id": r[2] or "",
            "ad_id": r[3] or "",
            "adset_name": r[4] or "",
            "invest": float(r[5] or 0),
            "clicks": int(r[6] or 0),
            "impressoes": int(r[7] or 0),
            "ctr": float(r[8] or 0),
            "vendas": int(r[9] or 0),
            "fat": float(r[10] or 0),
            "cpa": float(r[11] or 0),
            "roas": float(r[12] or 0)
        })

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
