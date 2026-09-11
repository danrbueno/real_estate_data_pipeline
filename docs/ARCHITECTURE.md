# 🏗️ Arquitetura do AI Scraper

## Sistema Geral

```
┌─────────────────────────────────────────────────────────────────────┐
│                    REAL ESTATE DATA PIPELINE                        │
│                                                                      │
│  Daily Airflow DAG (12 AM)                                          │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ Stage 1: DATA EXTRACTION (🤖 AI Powered)                    │   │
│  │                                                              │   │
│  │  ┌─────────────────────────────────────────────────────┐    │   │
│  │  │ AI Scraper Agent                                   │    │   │
│  │  │                                                     │    │   │
│  │  │  Rentals Task          Sales Task                  │    │   │
│  │  │  ├─ fetch listings     ├─ fetch listings          │    │   │
│  │  │  ├─ extract links      ├─ extract links           │    │   │
│  │  │  ├─ paginate (AI)      ├─ paginate (AI)           │    │   │
│  │  │  ├─ scrape details     ├─ scrape details          │    │   │
│  │  │  └─ save JSON          └─ save JSON               │    │   │
│  │  │                                                     │    │   │
│  │  └─────────────────────────────────────────────────────┘    │   │
│  │                         ↓                                    │   │
│  │  data/web/rentals.json     data/web/sales.json             │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                            ↓                                         │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ Stage 2: DATA TRANSFORMATION (Pandas)                        │   │
│  │                                                              │   │
│  │  ├─ Clean rentals data                                      │   │
│  │  ├─ Clean sales data                                        │   │
│  │  ├─ Join datasets                                           │   │
│  │  └─ Save CSV to staging                                     │   │
│  │                                                              │   │
│  │  data/staging/all_data.csv                                  │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                            ↓                                         │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │ Stage 3: DATA LOADING (SQLAlchemy → MySQL)                  │   │
│  │                                                              │   │
│  │  ├─ Reset database                                          │   │
│  │  ├─ Load data into tables                                   │   │
│  │  └─ Create indexes                                          │   │
│  │                                                              │   │
│  │  MySQL Database (Properties, Transactions, etc.)            │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Detalhe do AI Scraper

```
                    ┌───────────────────────────┐
                    │  DFImoveis Website        │
                    │ https://www.dfimoveis...  │
                    └────────────┬──────────────┘
                                 │
                    ┌────────────▼─────────────┐
                    │   HTTPClient             │
                    │                          │
                    │ - fetch(url)             │
                    │ - rate limiting (2s)     │
                    │ - error handling         │
                    │ - user agent config      │
                    └────────────┬──────────────┘
                                 │
                    ┌────────────▼──────────────────┐
                    │  AIScrapingAgent              │
                    │                               │
                    │ OpenAI GPT-4-turbo            │
                    │ (or gpt-3.5-turbo)            │
                    │                               │
                    │ Methods (reserved for a       │
                    │ future detail-field step):    │
                    │ ├─ extract_property_details() │
                    │ └─ extract_property_page_details() │
                    │                               │
                    │ Returns: JSON objects         │
                    └────────────┬──────────────────┘
                                 │
                    ┌────────────▼──────────────────┐
                    │  AdLinksCollector             │
                    │                               │
                    │ while has_pages:              │
                    │   ├─ fetch page HTML (HTTP)   │
                    │   ├─ extract links (regex)    │
                    │   ├─ if no links: stop        │
                    │   └─ next page                │
                    │ save all pages' links to JSON │
                    └────────────┬──────────────────┘
                                 │
                    ┌────────────▼──────────────────┐
                    │  Output Files                 │
                    │                               │
                    │ data/web/rentals.json:        │
                    │ {"title": "...",              │
                    │  "price": "...",              │
                    │  "area": "..."}               │
                    │                               │
                    │ data/web/sales.json:          │
                    │ {"title": "...",              │
                    │  "price": "...",              │
                    │  "area": "..."}               │
                    └───────────────────────────────┘
```

## Data Flow Completo

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│  ENTRADA: DFImoveis Website                                    │
│                                                                 │
│  ✓ URL: https://www.dfimoveis.com.br/venda/df/todos/apartamento
│  ✓ Tipo: sales / rentals                                      │
│  ✓ Dados: Imóveis com características e preços                │
│                                                                 │
└────────────────────────┬────────────────────────────────────────┘
                         │
                ┌────────▼────────┐
                │  AI Scraper     │
                │                 │
                │1. HTTPClient    │
                │   fetch HTML    │
                │                 │
                │2. AIAgent       │
                │   extract data  │
                │   (OpenAI)      │
                │                 │
                │3. Validate      │
                │   check fields  │
                │                 │
                │4. Save          │
                │   JSON file     │
                └────────┬────────┘
                         │
        ┌────────────────▼────────────────┐
        │  data/web/                      │
        │  ├─ rentals.json                │
        │  └─ sales.json                  │
        │                                 │
        │  Format: JSONL (1 line = 1 item)│
        │  Fields: title, price, area...  │
        └────────────────┬────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  Pandas Transform               │
        │  (rentals.py, sales.py)         │
        │                                 │
        │  - Clean data                   │
        │  - Normalize fields             │
        │  - Standardize formats          │
        │  - Handle missing values        │
        └────────────────┬────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  data/staging/                  │
        │  ├─ rentals.csv                 │
        │  ├─ sales.csv                   │
        │  └─ all_data.csv                │
        │                                 │
        │  Format: CSV (clean data)       │
        └────────────────┬────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  SQLAlchemy Load                │
        │  (database.py)                  │
        │                                 │
        │  - Connect MySQL                │
        │  - Reset tables                 │
        │  - Bulk insert                  │
        │  - Create indexes               │
        └────────────────┬────────────────┘
                         │
        ┌────────────────▼────────────────┐
        │  MySQL Database                 │
        │  ├─ properties table            │
        │  ├─ rentals table               │
        │  ├─ sales table                 │
        │  ├─ neighborhoods               │
        │  └─ cities                      │
        │                                 │
        │  SAÍDA: Dados persistidos       │
        └─────────────────────────────────┘
```

## Componentes do AI Scraper

```
┌──────────────────────────────────────────────────────────────┐
│                     AI SCRAPER MODULE                        │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ config.py                                            │   │
│  │ • OPENAI_API_KEY: chave da API                      │   │
│  │ • OPENAI_MODEL: modelo (gpt-4-turbo/gpt-3.5-turbo)  │   │
│  │ • DFIMOVEIS_BASE_URL: URL base                      │   │
│  │ • REQUEST_TIMEOUT: timeout HTTP (30s)              │   │
│  │ • REQUEST_DELAY: delay entre requisições (2s)      │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                   │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ http_client.py                                      │   │
│  │ ┌────────────────────────────────────────────────┐  │   │
│  │ │ HTTPClient class                              │  │   │
│  │ │ ├─ urllib.request (standard library)          │  │   │
│  │ │ ├─ delay: rate limiting                       │  │   │
│  │ │ └─ get(url): fetch com rate limiting          │  │   │
│  │ │                                               │  │   │
│  │ └────────────────────────────────────────────────┘  │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ ai_agent.py                                        │   │
│  │ ┌────────────────────────────────────────────────┐  │   │
│  │ │ AIScrapingAgent class                         │  │   │
│  │ │ ├─ client: OpenAI()                           │  │   │
│  │ │ ├─ model: "gpt-4-turbo"                       │  │   │
│  │ │ └─ Methods (reserved for a future             │  │   │
│  │ │    detail-field extraction step):             │  │   │
│  │ │    ├─ _call_openai(prompt)                    │  │   │
│  │ │    │  └─ chama API OpenAI                     │  │   │
│  │ │    ├─ extract_property_details(html, url)     │  │   │
│  │ │    │  └─ retorna dict com dados               │  │   │
│  │ │    ├─ extract_property_page_details(html, url)│  │   │
│  │ │    │  └─ retorna dict com dados da página      │  │   │
│  │ │    └─ validate_extraction(data)               │  │   │
│  │ │       └─ valida dados extraídos               │  │   │
│  │ └────────────────────────────────────────────────┘  │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ ad_links_collector.py                              │   │
│  │ ┌────────────────────────────────────────────────┐  │   │
│  │ │ AdLinksCollector class                        │  │   │
│  │ │ ├─ http_client: HTTPClient()                  │  │   │
│  │ │ └─ Methods:                                   │  │   │
│  │ │    ├─ collect(transaction_type)               │  │   │
│  │ │    │  ├─ loop through pages                   │  │   │
│  │ │    │  ├─ extract links (regex, no AI call)     │  │   │
│  │ │    │  └─ save links.json                       │  │   │
│  │ │    ├─ collect_page_ad_links(html)             │  │   │
│  │ │    │  └─ regex extraction, no AI call          │  │   │
│  │ │    └─ close()                                 │  │   │
│  │ │       └─ cleanup                              │  │   │
│  │ └────────────────────────────────────────────────┘  │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ main.py                                            │   │
│  │ • CLI entry point                                  │   │
│  │ • Argumento: --type                                │   │
│  │ • Uso: python -m app.ai_scraper.ad_links_collector.main --type rentals │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Fluxo de Execução

```
main.py
  │
  ├─ parse arguments (--type)
  │
  └─ AdLinksCollector.collect("rentals")
      │
      ├─ Loop: for page in pages:
      │   │
      │   ├─ HTTPClient.get(url)
      │   │   └─ fetch HTML com rate limiting
      │   │
      │   ├─ AdLinksCollector.collect_page_ad_links(html)
      │   │   └─ return [url1, url2, ...] (regex, sem chamada à IA)
      │   │
      │   └─ if not links: break (fim da paginação)
      │
      └─ save_links(transaction_type, pages)
          └─ grava data/raw/<tipo>/links.json

Output:
  ✓ data/raw/rentals/links.json (ou sales/links.json)
```

## Integração com Airflow

```
dag_pipeline_real_estate_ai.py
│
├─ DAG: dag_real_estate_data_pipeline_ai
│   schedule: "0 0 * * *" (diariamente 12 AM)
│
├─ Task: start_dag (EmptyOperator)
│   │
│   └─ TaskGroup: scrap
│       │
│       ├─ Task: scrap_rentals (PythonOperator)
│       │   └─ PythonCallable: scrap_rentals()
│       │       └─ AdLinksCollector().collect("rentals")
│       │
│       └─ Task: scrap_sales (PythonOperator)
│           └─ PythonCallable: scrap_sales()
│               └─ AdLinksCollector().collect("sales")
│
├─ TaskGroup: transform
│   ├─ Task: transform_rentals
│   ├─ Task: transform_sales
│   └─ Task: join_datasets
│
├─ TaskGroup: load
│   ├─ Task: reset_database
│   └─ Task: load_database
│
└─ Task: end_dag (EmptyOperator)
```

---

**Arquitetura completa e funcional!** ✅
