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
                    │ Methods:                      │
                    │ ├─ extract_property_links()   │
                    │ ├─ extract_pagination_info()  │
                    │ └─ extract_property_details() │
                    │                               │
                    │ Returns: JSON objects         │
                    └────────────┬──────────────────┘
                                 │
                    ┌────────────▼──────────────────┐
                    │  AIScraper Orchestrator       │
                    │                               │
                    │ while has_pages:              │
                    │   ├─ fetch page HTML          │
                    │   ├─ extract links            │
                    │   ├─ for each link:           │
                    │   │  ├─ fetch property HTML   │
                    │   │  ├─ extract details       │
                    │   │  ├─ validate data         │
                    │   │  └─ save JSON             │
                    │   ├─ check pagination        │
                    │   └─ next page                │
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
│  │ • MAX_PAGES: limite de páginas (None=todas)        │   │
│  └──────────────────────────────────────────────────────┘   │
│                          │                                   │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ http_client.py                                      │   │
│  │ ┌────────────────────────────────────────────────┐  │   │
│  │ │ HTTPClient class                              │  │   │
│  │ │ ├─ session: requests.Session()                │  │   │
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
│  │ │ └─ Methods:                                   │  │   │
│  │ │    ├─ _call_openai(prompt)                    │  │   │
│  │ │    │  └─ chama API OpenAI                     │  │   │
│  │ │    ├─ extract_property_links(html, base_url)  │  │   │
│  │ │    │  └─ retorna lista de URLs                │  │   │
│  │ │    ├─ extract_pagination_info(html)           │  │   │
│  │ │    │  └─ retorna {current, total, has_next}   │  │   │
│  │ │    ├─ extract_property_details(html, url)     │  │   │
│  │ │    │  └─ retorna dict com dados               │  │   │
│  │ │    └─ validate_extraction(data)               │  │   │
│  │ │       └─ valida dados extraídos               │  │   │
│  │ └────────────────────────────────────────────────┘  │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ scraper.py                                         │   │
│  │ ┌────────────────────────────────────────────────┐  │   │
│  │ │ AIScraper class (Orchestrator)                │  │   │
│  │ │ ├─ http_client: HTTPClient()                  │  │   │
│  │ │ ├─ ai_agent: AIScrapingAgent()                │  │   │
│  │ │ └─ Methods:                                   │  │   │
│  │ │    ├─ scrape_transaction_type(type)           │  │   │
│  │ │    │  ├─ loop through pages                   │  │   │
│  │ │    │  ├─ extract links                        │  │   │
│  │ │    │  ├─ scrape details                       │  │   │
│  │ │    │  └─ save JSON                            │  │   │
│  │ │    ├─ _save_property_to_json(data)            │  │   │
│  │ │    │  └─ append JSONL file                    │  │   │
│  │ │    └─ close()                                 │  │   │
│  │ │       └─ cleanup                              │  │   │
│  │ └────────────────────────────────────────────────┘  │   │
│  └──────────────────────┬───────────────────────────────┘   │
│                         │                                    │
│  ┌──────────────────────▼──────────────────────────────┐   │
│  │ main.py                                            │   │
│  │ • CLI entry point                                  │   │
│  │ • Argumentos: --type, --max-pages                  │   │
│  │ • Uso: python ai_scraper/main.py --type rentals  │   │
│  └──────────────────────────────────────────────────────┘   │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

## Fluxo de Execução

```
main.py
  │
  ├─ parse arguments (--type, --max-pages)
  │
  └─ AIScraper.scrape_transaction_type("rentals")
      │
      ├─ Loop: for page in pages:
      │   │
      │   ├─ HTTPClient.get(url)
      │   │   └─ fetch HTML com rate limiting
      │   │
      │   ├─ AIScrapingAgent.extract_property_links(html)
      │   │   ├─ _call_openai(prompt)
      │   │   │  └─ OpenAI GPT-4 processa HTML
      │   │   └─ return [url1, url2, ...]
      │   │
      │   ├─ AIScrapingAgent.extract_pagination_info(html)
      │   │   └─ return {current_page, total_pages, has_next}
      │   │
      │   ├─ Loop: for property_url in links:
      │   │   │
      │   │   ├─ HTTPClient.get(property_url)
      │   │   │   └─ fetch property HTML
      │   │   │
      │   │   ├─ AIScrapingAgent.extract_property_details(html)
      │   │   │   ├─ _call_openai(prompt)
      │   │   │   │  └─ extract: title, price, area, ...
      │   │   │   └─ return dict
      │   │   │
      │   │   ├─ AIScrapingAgent.validate_extraction(data)
      │   │   │   └─ check required fields
      │   │   │
      │   │   └─ _save_property_to_json(data)
      │   │       └─ append to file (JSONL format)
      │   │
      │   └─ if not has_next_page: break
      │
      └─ close()
          └─ cleanup resources

Output:
  ✓ data/web/rentals.json (ou sales.json)
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
│       │       └─ AIScraper().scrape_transaction_type("rentals")
│       │
│       └─ Task: scrap_sales (PythonOperator)
│           └─ PythonCallable: scrap_sales()
│               └─ AIScraper().scrape_transaction_type("sales")
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
