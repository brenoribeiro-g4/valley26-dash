# G4 Valley 2026 Dashboard — Session Context

## Project
- **Repo:** https://github.com/brenoribeiro-g4/valley26-dash
- **Deploy:** https://valley26-dash.vercel.app
- **Stack:** Single-file HTML + Chart.js + Vercel Serverless (Python)

## Pendentes
1. **Page 3** — conectar API real + novo filtro "Mídia Paga" (somatório FB+GG+YT) + filtros de canal funcionais
2. **Gráfico V25** na Page 2 que não renderiza barras (dados existem no JSON)
3. Ajustes visuais pontuais

## Referência Técnica
- **Databricks host:** https://dbc-8acefaf9-a170.cloud.databricks.com
- **Warehouse:** bbae754ea44f67e0
- **Edition:** g4valley-1026
- **Período Lote 01:** 07/07 → 31/07/2026
- **Meta total:** R$ 1.000.000 (833 ingressos × R$ 1.200)
- **Meta Ads filter:** utm_source='facebook' + campaign LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'
- **Google Ads filter:** campaign LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%'
- **V25 Lote 01:** edicao_do_evento='g4valley-1125', tipo_de_ingresso IN ('comum','vip','atlas','experience'), dt_event 14/08→07/09/2025

## Arquitetura
- `index.html` — dashboard completo (3 páginas, CSS, JS, inline data)
- `api/data.py` — Vercel serverless function (queries Databricks live)
- `g4valley-fetch.py` — script batch para gerar JSON + embed no HTML
- `AGENTS.md` — documentação completa do projeto

## Canal Classification (SQL CASE)
- instagram/ig → reclassificado por utm_medium (g4_social, tg_social, alfredo, nardon)
- facebook + campaign específica → meta_ads
- facebook outros → facebook_other
- google + campaign específica → google_ads

## Env Vars (Vercel)
- DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID
