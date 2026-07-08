# G4 Valley 2026 — Dashboard de Aquisição

Dashboard de acompanhamento de vendas e captação para o G4 Valley 2026 (Lote 01).

## Stack

- **Frontend:** Single-file HTML + Chart.js (zero build, zero dependencies)
- **Data:** JSON estático gerado por script Python que consulta Databricks
- **Deploy:** Vercel (static)

## Estrutura

```
├── index.html          # Dashboard completo (3 páginas)
├── g4valley-fetch.py   # Script de extração de dados do Databricks
├── g4valley-data.json  # Dados (gerado pelo script, não versionado)
└── README.md
```

## Como usar

### 1. Atualizar dados

```bash
# Defina as variáveis de ambiente (ou use os defaults do script)
export DATABRICKS_HOST="https://dbc-8acefaf9-a170.cloud.databricks.com"
export DATABRICKS_TOKEN="seu_token_aqui"
export DATABRICKS_WAREHOUSE_ID="bbae754ea44f67e0"

# Rode o script
python3 g4valley-fetch.py
```

O script:
- Consulta vendas, leads e investimento no Databricks
- Gera `g4valley-data.json`
- Embute os dados diretamente no `index.html` (fallback para file://)

### 2. Visualizar localmente

Abra `index.html` diretamente no browser, ou:

```bash
python3 -m http.server 8080
# Acesse http://localhost:8080
```

### 3. Deploy

Push para o repositório — Vercel detecta e faz deploy automático do `index.html`.

## Fontes de dados (Databricks)

| Dado | Tabela |
|------|--------|
| Vendas/Faturamento | `g4_eventos_lancamentos.vw_mart_eventos_orders` |
| Leads | `g4_eventos_lancamentos.vw_mart_eventos_leads_pre_inscricao` |
| Investimento Ads | `production.gold.marketing_fct` |

## Filtros de campanha

- **Meta Ads:** `utm_source = 'facebook'` + `utm_campaign LIKE '%_adsfb_gtm_g4valley26_vendas_carrinhoaberto_alwayson%'`
- **Google Ads:** `utm_campaign LIKE '%_adsgg_gtm_g4valley26_carrinhoaberto_vendas_%'`
- **Edição:** `g4valley-1026`
- **Lote 01:** 07/07/2026 → 28/07/2026

---

G4 Educação · 2026
