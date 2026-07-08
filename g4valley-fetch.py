#!/usr/bin/env python3
"""
G4 Valley 2026 — Dashboard Fetch Script
Queries Databricks SQL and generates g4valley-data.json

Usage:
    python3 g4valley-fetch.py

Config:
    - Edition: g4valley-1026
    - Lote 01: 2026-07-07 → 2026-07-28
    - Tables:
        - Vendas: g4_eventos_lancamentos.vw_mart_eventos_orders
        - Leads:  g4_eventos_lancamentos.vw_mart_eventos_leads_pre_inscricao
    - Refresh: every 15-30 minutes (cron or manual)
"""

import json
import os
import time
import requests
from datetime import datetime, date

# ============================================================
# CONFIGURATION
# ============================================================
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "https://dbc-8acefaf9-a170.cloud.databricks.com")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "bbae754ea44f67e0")

if not DATABRICKS_TOKEN:
    raise SystemExit("ERROR: Set DATABRICKS_TOKEN environment variable")

EDITION = "g4valley-1026"
LOTE_START = "2026-07-07"
LOTE_END = "2026-07-31"

OUTPUT_FILE = "g4valley-data.json"

HEADERS = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# ============================================================
# DATABRICKS SQL HELPER
# ============================================================
def run_query(sql, timeout=50):
    """Execute a SQL statement and wait for results."""
    url = f"{DATABRICKS_HOST}/api/2.0/sql/statements"
    payload = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": f"{timeout}s"
    }
    resp = requests.post(url, headers=HEADERS, json=payload)
    data = resp.json()

    state = data.get("status", {}).get("state", "")

    # If pending, poll
    if state in ("PENDING", "RUNNING"):
        stmt_id = data.get("statement_id")
        for _ in range(60):
            time.sleep(5)
            poll = requests.get(f"{url}/{stmt_id}", headers=HEADERS).json()
            state = poll.get("status", {}).get("state", "")
            if state == "SUCCEEDED":
                return poll["result"]["data_array"]
            elif state == "FAILED":
                raise Exception(f"Query failed: {poll['status'].get('error', {})}")
        raise Exception("Query timeout after 5 minutes")

    if state == "SUCCEEDED":
        return data["result"]["data_array"]

    error = data.get("status", {}).get("error", data)
    raise Exception(f"Query error: {error}")


# ============================================================
# QUERIES
# ============================================================
def fetch_vendas_totais():
    """Total sales metrics for Lote 01 period."""
    sql = f"""
    SELECT
        COUNT(*) as vendas,
        ROUND(SUM(vl_venda), 2) as faturamento,
        ROUND(SUM(vl_bruto), 2) as bruto,
        ROUND(AVG(vl_venda), 2) as ticket_medio
    FROM g4_eventos_lancamentos.vw_mart_eventos_orders
    WHERE edicao_do_evento = '{EDITION}'
      AND dt_event >= '{LOTE_START}'
      AND dt_event <= '{LOTE_END}'
    """
    return run_query(sql)


def fetch_vendas_by_day():
    """Daily sales breakdown."""
    sql = f"""
    SELECT
        CAST(dt_event AS STRING) as dia,
        COUNT(*) as vendas,
        ROUND(SUM(vl_venda), 2) as faturamento,
        ROUND(SUM(vl_bruto), 2) as bruto
    FROM g4_eventos_lancamentos.vw_mart_eventos_orders
    WHERE edicao_do_evento = '{EDITION}'
      AND dt_event >= '{LOTE_START}'
      AND dt_event <= '{LOTE_END}'
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(sql)


def fetch_vendas_by_canal():
    """Sales by channel (with instagram/ig reclassified as organic)."""
    sql = f"""
    SELECT
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
        END as canal,
        COUNT(*) as vendas,
        ROUND(SUM(vl_venda), 2) as faturamento
    FROM g4_eventos_lancamentos.vw_mart_eventos_orders
    WHERE edicao_do_evento = '{EDITION}'
      AND dt_event >= '{LOTE_START}'
      AND dt_event <= '{LOTE_END}'
    GROUP BY 1
    ORDER BY 3 DESC
    """
    return run_query(sql)


def fetch_vendas_by_day_canal():
    """Daily sales by channel (with reclassification)."""
    sql = f"""
    SELECT
        CAST(dt_event AS STRING) as dia,
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
        END as canal,
        COUNT(*) as vendas,
        ROUND(SUM(vl_venda), 2) as faturamento
    FROM g4_eventos_lancamentos.vw_mart_eventos_orders
    WHERE edicao_do_evento = '{EDITION}'
      AND dt_event >= '{LOTE_START}'
      AND dt_event <= '{LOTE_END}'
    GROUP BY 1, 2
    ORDER BY 1, 4 DESC
    """
    return run_query(sql)


def fetch_vendas_by_hour():
    """Hourly sales distribution with reclassified channels."""
    sql = f"""
    SELECT
        HOUR(ts_event) as hora,
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
        END as canal,
        COUNT(*) as vendas,
        ROUND(SUM(vl_venda), 2) as faturamento
    FROM g4_eventos_lancamentos.vw_mart_eventos_orders
    WHERE edicao_do_evento = '{EDITION}'
      AND dt_event >= '{LOTE_START}'
      AND dt_event <= '{LOTE_END}'
    GROUP BY 1, 2
    ORDER BY 1, 4 DESC
    """
    return run_query(sql)


def fetch_marketing_invest():
    """Marketing investment by day and source for Valley campaigns."""
    sql = f"""
    SELECT
        CAST(event_at AS STRING) as dia,
        COALESCE(utm_source, 'nao-definido') as canal,
        event,
        ROUND(SUM(event_value), 2) as total
    FROM production.gold.marketing_fct
    WHERE (
        LOWER(utm_campaign) LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'
        OR LOWER(utm_campaign) LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%'
    )
      AND event_at >= '{LOTE_START}'
      AND event_at <= '{LOTE_END}'
    GROUP BY 1, 2, 3
    ORDER BY 1, 2, 3
    """
    return run_query(sql)


def fetch_marketing_totals():
    """Total marketing metrics for Valley campaigns."""
    sql = f"""
    SELECT
        COALESCE(utm_source, 'nao-definido') as canal,
        event,
        ROUND(SUM(event_value), 2) as total
    FROM production.gold.marketing_fct
    WHERE (
        LOWER(utm_campaign) LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'
        OR LOWER(utm_campaign) LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%'
    )
      AND event_at >= '{LOTE_START}'
      AND event_at <= '{LOTE_END}'
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    return run_query(sql)


def fetch_ads_report():
    """Full ads report: metrics from marketing_fct + sales from orders via utm_content match."""
    sql = f"""
    WITH ads_metrics AS (
        SELECT
            a.ad_name,
            LEFT(m.utm_campaign, 100) as campaign,
            m.adset_id,
            m.ad_id,
            ROUND(SUM(CASE WHEN m.event='investimento' THEN m.event_value ELSE 0 END), 2) as invest,
            ROUND(SUM(CASE WHEN m.event='clicks' THEN m.event_value ELSE 0 END), 0) as clicks,
            ROUND(SUM(CASE WHEN m.event='impressoes' THEN m.event_value ELSE 0 END), 0) as impressoes,
            ROUND(SUM(CASE WHEN m.event='reach' THEN m.event_value ELSE 0 END), 0) as reach
        FROM production.gold.marketing_fct m
        LEFT JOIN production.gold.ads_details a ON m.ad_id = a.ad_id
        WHERE LOWER(m.utm_campaign) LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'
          AND m.event_at >= '{LOTE_START}'
          AND m.event_at <= '{LOTE_END}'
        GROUP BY 1, 2, 3, 4
    ),
    vendas AS (
        SELECT
            LOWER(utm_content) as ad_lower,
            COUNT(*) as vendas,
            ROUND(SUM(vl_venda), 2) as fat
        FROM g4_eventos_lancamentos.vw_mart_eventos_orders
        WHERE edicao_do_evento = '{EDITION}'
          AND dt_event >= '{LOTE_START}'
          AND dt_event <= '{LOTE_END}'
          AND utm_source = 'facebook'
        GROUP BY 1
    )
    SELECT
        am.ad_name,
        am.campaign,
        am.adset_id,
        am.ad_id,
        am.invest,
        am.clicks,
        am.impressoes,
        ROUND(am.clicks / NULLIF(am.impressoes, 0) * 100, 2) as ctr,
        COALESCE(v.vendas, 0) as vendas,
        COALESCE(v.fat, 0) as fat,
        CASE WHEN COALESCE(v.vendas,0) > 0 THEN ROUND(am.invest / v.vendas, 2) ELSE 0 END as cpa,
        CASE WHEN am.invest > 0 THEN ROUND(COALESCE(v.fat,0) / am.invest, 2) ELSE 0 END as roas
    FROM ads_metrics am
    LEFT JOIN vendas v ON v.ad_lower = LOWER(am.ad_name)
    ORDER BY am.invest DESC
    LIMIT 100
    """
    return run_query(sql)


def fetch_meta_ads_vendas():
    """Meta Ads vendas: source=facebook + specific campaigns only."""
    sql = f"""
    SELECT
        ROUND(SUM(vl_venda), 2) as fat,
        COUNT(*) as vendas
    FROM g4_eventos_lancamentos.vw_mart_eventos_orders
    WHERE edicao_do_evento = '{EDITION}'
      AND dt_event >= '{LOTE_START}'
      AND dt_event <= '{LOTE_END}'
      AND utm_source = 'facebook'
      AND LOWER(utm_campaign) LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'
    """
    return run_query(sql)


def fetch_google_ads_vendas():
    """Google Ads vendas: source=google + specific campaigns only."""
    sql = f"""
    SELECT
        ROUND(SUM(vl_venda), 2) as fat,
        COUNT(*) as vendas
    FROM g4_eventos_lancamentos.vw_mart_eventos_orders
    WHERE edicao_do_evento = '{EDITION}'
      AND dt_event >= '{LOTE_START}'
      AND dt_event <= '{LOTE_END}'
      AND utm_source = 'google'
      AND LOWER(utm_campaign) LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%'
    """
    return run_query(sql)


def fetch_leads_totais():
    """Total leads for the edition (all time, not just lote)."""
    sql = f"""
    SELECT
        COUNT(*) as total_leads
    FROM g4_eventos_lancamentos.vw_mart_eventos_leads_pre_inscricao
    WHERE cl_edicao_evento_pre_inscricao = '{EDITION}'
    """
    return run_query(sql)


def fetch_leads_by_day():
    """Daily leads."""
    sql = f"""
    SELECT
        CAST(DATE(ts_inscricao_evento) AS STRING) as dia,
        COUNT(*) as leads
    FROM g4_eventos_lancamentos.vw_mart_eventos_leads_pre_inscricao
    WHERE cl_edicao_evento_pre_inscricao = '{EDITION}'
      AND DATE(ts_inscricao_evento) >= '{LOTE_START}'
      AND DATE(ts_inscricao_evento) <= '{LOTE_END}'
    GROUP BY 1
    ORDER BY 1
    """
    return run_query(sql)


def fetch_leads_by_canal():
    """Leads by utm_source."""
    sql = f"""
    SELECT
        COALESCE(utm_source, 'nao-definido') as canal,
        COUNT(*) as leads
    FROM g4_eventos_lancamentos.vw_mart_eventos_leads_pre_inscricao
    WHERE cl_edicao_evento_pre_inscricao = '{EDITION}'
      AND DATE(ts_inscricao_evento) >= '{LOTE_START}'
      AND DATE(ts_inscricao_evento) <= '{LOTE_END}'
    GROUP BY 1
    ORDER BY 2 DESC
    """
    return run_query(sql)


# ============================================================
# BUILD JSON
# ============================================================
def build_json():
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Fetching data for {EDITION} (Lote 01: {LOTE_START} -> {LOTE_END})...")

    # 1. Totais
    print("  → vendas totais...")
    totais = fetch_vendas_totais()
    total_vendas = int(totais[0][0])
    total_fat = float(totais[0][1] or 0)
    total_bruto = float(totais[0][2] or 0)
    ticket_medio = float(totais[0][3] or 0)

    # 2. By day
    print("  → vendas por dia...")
    by_day_raw = fetch_vendas_by_day()
    by_day = {}
    for row in by_day_raw:
        by_day[row[0]] = {
            "fat": float(row[2] or 0),
            "vendas": int(row[1]),
            "bruto": float(row[3] or 0)
        }

    # 3. By canal
    print("  → vendas por canal...")
    by_canal_raw = fetch_vendas_by_canal()
    by_canal = {}
    for row in by_canal_raw:
        by_canal[row[0]] = {
            "fat": float(row[2] or 0),
            "vendas": int(row[1])
        }

    # 4. By day x canal
    print("  → vendas por dia x canal...")
    by_day_canal_raw = fetch_vendas_by_day_canal()
    by_day_canal = {}
    for row in by_day_canal_raw:
        dia = row[0]
        canal = row[1]
        if dia not in by_day_canal:
            by_day_canal[dia] = {}
        by_day_canal[dia][canal] = {
            "fat": float(row[3] or 0),
            "vendas": int(row[2])
        }

    # 5. By hour
    print("  → vendas por hora...")
    by_hour_raw = fetch_vendas_by_hour()
    by_hour = {}
    for row in by_hour_raw:
        h = str(int(row[0])).zfill(2)
        canal = row[1]
        fat = float(row[3] or 0)
        vendas = int(row[2])
        if h not in by_hour:
            by_hour[h] = {"fat": 0, "vendas": 0, "by_canal": {}}
        by_hour[h]["fat"] += fat
        by_hour[h]["vendas"] += vendas
        by_hour[h]["by_canal"][canal] = fat

    # 6. Marketing (invest, impressions, clicks)
    print("  -> marketing (invest, impressions, clicks)...")
    mkt_totals_raw = fetch_marketing_totals()
    mkt_by_canal = {}
    total_invest = 0
    total_impressoes = 0
    total_clicks = 0
    for row in mkt_totals_raw:
        canal, event, val = row[0], row[1], float(row[2] or 0)
        if canal not in mkt_by_canal:
            mkt_by_canal[canal] = {"investimento": 0, "impressoes": 0, "clicks": 0, "reach": 0}
        mkt_by_canal[canal][event] = val
        if event == "investimento": total_invest += val
        elif event == "impressoes": total_impressoes += val
        elif event == "clicks": total_clicks += val

    mkt_by_day_raw = fetch_marketing_invest()
    invest_by_day = {}
    for row in mkt_by_day_raw:
        dia, canal, event, val = row[0], row[1], row[2], float(row[3] or 0)
        if event == "investimento":
            invest_by_day[dia] = invest_by_day.get(dia, 0) + val

    for dia, inv in invest_by_day.items():
        if dia in by_day:
            by_day[dia]["invest"] = round(inv, 2)
        else:
            by_day[dia] = {"fat": 0, "vendas": 0, "bruto": 0, "invest": round(inv, 2)}

    # 7. Leads
    # 8. Ads Report (Page 3)
    print("  -> ads report (campaigns x vendas)...")
    ads_raw = fetch_ads_report()
    ads_report = []
    for row in ads_raw:
        ads_report.append({
            "ad_name": row[0] or "",
            "campaign": row[1] or "",
            "adset_id": row[2] or "",
            "ad_id": row[3] or "",
            "invest": float(row[4] or 0),
            "clicks": int(float(row[5] or 0)),
            "impressoes": int(float(row[6] or 0)),
            "ctr": float(row[7] or 0),
            "vendas": int(row[8] or 0),
            "fat": float(row[9] or 0),
            "cpa": float(row[10] or 0),
            "roas": float(row[11] or 0)
        })

    # 9. Leads
    print("  -> leads totais + por dia + por canal...")
    leads_total = int(fetch_leads_totais()[0][0])
    leads_by_day_raw = fetch_leads_by_day()
    leads_by_day = {row[0]: int(row[1]) for row in leads_by_day_raw}
    leads_by_canal_raw = fetch_leads_by_canal()
    leads_by_canal = {row[0]: int(row[1]) for row in leads_by_canal_raw}

    # 7. Performance windows
    today_str = date.today().isoformat()
    yesterday_str = date(date.today().year, date.today().month, date.today().day - 1).isoformat() if date.today().day > 1 else date.today().isoformat()

    today_data = by_day.get(today_str, {"fat": 0, "vendas": 0})
    yesterday_data = by_day.get(yesterday_str, {"fat": 0, "vendas": 0})

    # Geral D-1 (all except today)
    ovd1_fat = sum(d["fat"] for k, d in by_day.items() if k < today_str)
    ovd1_vendas = sum(d["vendas"] for k, d in by_day.items() if k < today_str)

    # Investment by period
    invest_hoje = invest_by_day.get(today_str, 0)
    invest_d1 = invest_by_day.get(yesterday_str, 0)
    invest_ovd1 = sum(v for k, v in invest_by_day.items() if k < today_str)

    def calc_perf(fat, vendas, invest):
        return {
            "invest": round(invest, 2),
            "vendas": vendas,
            "fat": round(fat, 2),
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

    # Perf by paid channel
    # Meta Ads = utm_source='facebook' + specific campaign patterns ONLY
    # Google Ads = utm_source='google' + specific campaign patterns ONLY
    perf_canal = {}

    # Fetch Meta Ads vendas with strict filter
    meta_ads_vendas = fetch_meta_ads_vendas()
    meta_fat = sum(float(row[0] or 0) for row in meta_ads_vendas)
    meta_vendas_count = int(meta_ads_vendas[0][1]) if meta_ads_vendas and meta_ads_vendas[0][1] else 0
    meta_invest = mkt_by_canal.get("facebook", {}).get("investimento", 0)
    perf_canal["meta_ads"] = calc_perf(meta_fat, meta_vendas_count, meta_invest)

    # Google Ads
    google_ads_vendas = fetch_google_ads_vendas()
    google_fat = sum(float(row[0] or 0) for row in google_ads_vendas)
    google_vendas_count = int(google_ads_vendas[0][1]) if google_ads_vendas and google_ads_vendas[0][1] else 0
    google_invest = mkt_by_canal.get("google", {}).get("investimento", 0)
    perf_canal["google"] = calc_perf(google_fat, google_vendas_count, google_invest)

    # YouTube (no data yet)
    perf_canal["youtube"] = calc_perf(0, 0, 0)

    # Build final JSON
    output = {
        "updated_at": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "edition": EDITION,
        "lote_start": LOTE_START,
        "lote_end": LOTE_END,
        "total_fat": total_fat,
        "total_bruto": total_bruto,
        "total_vendas": total_vendas,
        "total_invest": round(total_invest, 2),
        "ticket_medio": ticket_medio,
        "total_leads": leads_total,
        "total_impressoes": total_impressoes,
        "total_clicks": total_clicks,
        "by_day": by_day,
        "by_canal": by_canal,
        "by_day_canal": by_day_canal,
        "by_hour": by_hour,
        "leads_by_day": leads_by_day,
        "leads_by_canal": leads_by_canal,
        "perf": perf,
        "perf_canal": perf_canal,
        "mkt_by_canal": {k: {kk: round(vv, 2) for kk, vv in v.items()} for k, v in mkt_by_canal.items()},
        "ads_report": ads_report,
        "raw_rows": []
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    # Embed into index.html for file:// fallback
    import re
    html_path = "index.html"
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            html = f.read()
        compact = json.dumps(output, ensure_ascii=False)
        html = re.sub(
            r'(<script type="application/json" id="inlineData">)(.*?)(</script>)',
            lambda m: m.group(1) + compact + m.group(3),
            html, flags=re.DOTALL
        )
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        print("  + Dados embutidos no index.html (file:// OK)")
    except Exception as e:
        print(f"  ! Nao embuti no HTML: {e}")

    print(f"\nDone! Written to {OUTPUT_FILE}")
    print(f"  Vendas: {total_vendas} | Fat: R$ {total_fat:,.2f} | Ticket: R$ {ticket_medio:,.2f}")
    print(f"  Leads (lote): {sum(leads_by_day.values())} | Leads (total): {leads_total}")
    print(f"  Dias com dados: {len(by_day)} | Canais: {len(by_canal)}")


if __name__ == "__main__":
    build_json()
