# Release Notes

## Release Readiness Summary

- **Status:** `NO-GO` for production deployment (deployment target/command/rollback undefined); pre-deployment code quality gate **passes**.
- **Evidence:** `python -m pytest -c app/pytest.ini` → 35 passed, 0 failed, 100% coverage (584 statements; required 90%, per `app/.coveragerc`).
- **Blockers to deployment:** no defined production target, release/deploy command, secret-management mechanism, monitoring, or rollback procedure exist in this repository. `docker-compose.postgres.yml` provisions a local/dev Postgres container only, not a production deployment. The `.github/skills/deploy-full-coverage/SKILL.md` referenced by `.github/agents/release-manager.agent.md` does not exist in the repo.
- See [Current Release-Prep Pass](#current-release-prep-pass-2026-09-11) below for what was fixed to reach a green quality gate.

## Scope

This document records changes made since the creation of the `feature/scrapy-to-ai` branch, starting from commit base `0f299fb`. It also includes uncommitted local changes, identified below as **Current Release-Prep Pass**.

## Current Release-Prep Pass (2026-09-11)

The listing-page downloader package was renamed (outside this pass) from `main_pages_downloader` to `main_page_scraper` and then to its current name, `app/ai_scraper/ad_links_collector/`, with class `AdLinksCollector` and method `collect(transaction_type)` (previously `AIScraper.scrape_transaction_type` / `MainPageScraper.scrap_ad_type`). That rename left several call sites out of sync, which is what this pass found and fixed:

- Fixed `AttributeError`-causing stale calls to the removed `scrap_ad_type` method — updated to `collect` in `app/airflow/dags/dag_pipeline_real_estate_ai.py` (both Airflow task callables), `docs/example_usage.py` (all three example functions), and `app/tests/test_ai_scraper.py`.
- Fixed stale test references to the old `MainPageScraper` class name and the old `app.ai_scraper.main_page_scraper.main` module path (`test_config_defaults_and_exports`, `test_main_returns_expected_exit_codes`, `test_main_module_exits_with_cli_status`, `test_downloader_scripts_support_direct_execution`).
- Rewrote `app/ai_scraper/README.md`'s usage examples, architecture tree, and troubleshooting section to reference `ad_links_collector`/`AdLinksCollector`/`collect` instead of the stale names.
- Verified via `python -m pytest -c app/pytest.ini` (the exact command used by `.github/workflows/release-quality.yml`): 35 passed, 0 failed, 100% coverage.
- Smoke-tested both CLIs (`ad_links_collector/main.py --help`, `property_pages_downloader/main.py --help`) and the public package import (`from app.ai_scraper import AdLinksCollector, AIScrapingAgent, PropertyPagesDownloader`) — all succeed.
- Confirmed no production deployment target, release command, secret-management mechanism, or rollback procedure is defined anywhere in the repository; this blocks any `DEPLOYED` status regardless of code quality gate results.

## Changes Prior To This Pass

The following changes were made earlier in the same branch (previously uncommitted, now part of commit `bfc54e0`):

- `main_pages_downloader` no longer persists listing pages to Postgres. The `tb_main_pages` table, `app/ai_scraper/db.py`, and `app/ai_scraper/models.py` (the `MainPage` model) were removed entirely, along with the old `AIScraper.count_properties_in_html` and `AIScraper.save_page_html`.
- The listing-page downloader now fetches each page's HTML directly via `HTTPClient` (plain HTTP, no OpenAI calls) and extracts ad links with a local regex. It stops once a page's fetch fails or returns zero ad links.
- Instead of saving page HTML, the listing-page downloader writes every page's ad links to a single JSON file at `data/raw/<rentals|sales>/links.json`, shaped as `[{"page": 1, "links": ["https://www.dfimoveis.com.br/imovel/...", "..."]}]`.
- `ai_agent.py` was simplified: `download_page`, `_is_valid_html`, and `extract_pagination_info` were removed as dead code. An AI web-search-based link-extraction method was also tried (asking the model to open a URL and return its ad links in a single call) but was reverted after it proved unreliable in real runs — it fabricated placeholder links on some pages and returned only a fraction of the real ads on others. The AI agent is no longer involved in listing-page crawling at all.
- Added a shared `_parse_json_content` helper in `ai_agent.py`, reused by `_call_openai` for unwrapping JSON from markdown fences or surrounding text.
- `property_pages_downloader`'s link-extraction regex was aligned with the listing-page downloader's (broader pattern covering absolute/relative hrefs without quotes, trailing punctuation trimmed).
- `http_client.py` was rewritten to use the Python standard library (`urllib.request`) instead of the `requests` package, and the `requests` dependency was dropped from `config/requirements.txt`.
- `app/.coveragerc`'s `fail_under` threshold was lowered from 100 to 90.
- The `--max-pages`/`-m` CLI option was removed from both downloader CLIs, along with the `MAX_PAGES` config constant and the `max_pages` parameter on `PropertyPagesDownloader._load_page_paths`/`extract_transaction_type` — the option was never going to be used in this project.
- Removed dead/unused code found during cleanup: the unused `PROCESSED_DATA_DIR` config constant, the unused `beautifulsoup4`/`lxml` dependencies (never imported anywhere), and test scaffolding (`FakeExtractionAgent`) that only exercised itself rather than any real code path.

## Changes Prior to Latest Commit

The following changes were made prior to commit `30324a3` (already committed):

- The two scraping agents were separated into independent packages:
  - `app/ai_scraper/main_pages_downloader/`: downloads and saves paginated listing pages.
  - `app/ai_scraper/property_pages_downloader/`: reads saved listing pages, downloads linked property-detail HTML, and saves it under `data/raw/<type>/properties/`.
- The property downloader no longer calls OpenAI or writes extracted property JSON during the download step.
- The downloader modules were renamed:
  - `scraper.py` became `main_pages_downloader.py`.
  - `data_extractor.py` became `property_pages_downloader.py`.
- The property downloader class was renamed from `PropertyDataExtractor` to `PropertyPagesDownloader`.
- Public exports, imports, tests, and documentation were updated for the new package and class names.
- Both downloader CLIs support direct execution from the repository root and module execution:

```powershell
python app\ai_scraper\main_pages_downloader\main.py -t rentals
python -m app.ai_scraper.main_pages_downloader.main -t rentals
python app\ai_scraper\property_pages_downloader\main.py -t rentals
```

- A workspace skill was added at `.github/skills/configure-mongodb-docker/SKILL.md`, covering MongoDB Docker/Compose setup, credentials, persistence, healthchecks, validation, backups, and troubleshooting.

## Main Change: Scrapy to AI Scraper

- The Scrapy-based data collection mechanism was removed.
- The pipeline now uses the OpenAI-based `ai_scraper` module to collect DFImoveis real-estate pages.
- The `dag_real_estate_data_pipeline_ai` DAG was added to collect rental and sale data before transformation, consolidation, and database loading.
- Collection uses an HTTP client with rate limiting, failure handling, and raw HTML page persistence.
- The first agent downloads listing pages and the second agent downloads the linked property-detail HTML pages.
- OpenAI extraction remains available in `AIScrapingAgent` but is not invoked by the property-page download agent.

## Structure and Configuration

- Application code was organized under `app/`:
  - `app/ai_scraper/`: shared HTTP client, OpenAI agent, configuration, and public exports.
  - `app/ai_scraper/main_pages_downloader/`: listing-page downloader and CLI.
  - `app/ai_scraper/property_pages_downloader/`: property-page downloader and CLI.
  - `app/airflow/dags/`: Airflow DAG and data-pipeline modules.
  - `app/tests/`: automated tests.
- `config/requirements.txt` was created for application and test dependencies.
- OpenAI is constrained to `>=1.3.0,<2.0.0`, matching the client API used by the code.
- Pydantic is constrained to `>=1.10.0,<2.0.0`, matching the Apache Airflow 2.6.3 version documented by the project.
- The environment-variable template was moved to `config/.env.example`.
- Shared configuration now checks `config/.env`, `app/.env`, and the project-root `.env`.
- Generated data files, scraped pages, and Python bytecode were removed from version control.

## Quality and Testing

- A deterministic AI Scraper test suite was added at `app/tests/test_ai_scraper.py`.
- Tests cover:
  - HTTP requests, rate limiting, and network errors;
  - OpenAI response parsing, including JSON, Markdown blocks, invalid responses, and exceptions;
  - extracted-data validation and property-detail extraction prompts;
  - listing-page link extraction and `links.json` persistence;
  - pagination stop conditions;
  - CLI results and exit codes.
- Pytest configuration is located at `app/pytest.ini`.
- Coverage configuration is located at `app/.coveragerc`, with branch coverage and a 90% minimum.
- Current focused validation command:

```powershell
python -m pytest --no-cov -q app/tests/test_ai_scraper.py
```

- Latest local result: 35 passing tests with 100% line and branch coverage across `app` (required minimum: 90%).

## CI and Release

- The `.github/workflows/release-quality.yml` workflow was added.
- The workflow installs dependencies and runs `python -m pytest -c app/pytest.ini` for pull requests and pushes to `main`.
- The `.github/agents/release-manager.agent.md` agent was added to issue release decisions and require explicit approval before a production deployment. It references a `.github/skills/deploy-full-coverage/SKILL.md` skill that does not currently exist in the repository — this should be created or the reference removed before relying on the agent for automated release gating.

## Documentation

- The root README was updated to reflect the AI Scraper architecture.
- Architecture, quick-start, project-structure, optimization, and checklist guides were added under `docs/`.
- Historical Scrapy references were removed from active documentation and code comments.

## In Progress and Limitations

- The coverage gate (`app/.coveragerc`) currently requires a 90% minimum across `app`, not just `app.ai_scraper`.
- The Airflow DAG, Pandas transformations, and ORM modules under `app/airflow/` still require deterministic tests before full pipeline coverage can be enforced.
- The repository does not yet define a production target, deployment command, secret-management mechanism, monitoring, or rollback procedure. Production deployment is therefore neither authorized nor fully specified.
- The CLI tests emit non-blocking `runpy` warnings because the CLI modules are imported before being executed with `runpy`.
- The official coverage gate now passes with the repository configuration; the CLI tests still emit only non-blocking `runpy` warnings.

## Branch Commit History

| Commit | Description |
| --- | --- |
| `3d7fbb9` | Update the README to reflect AI-based data collection. |
| `c69a0f0` | Correct workflow-name formatting for tests and pull requests. |
| `f7ca7a2` | Add AI Scraper usage examples and initial tests. |
| `f358151` | Add the AI Scraper module and comprehensive documentation. |
| `fb066c8` | Implement the AI-based scraper and its corresponding Airflow DAG. |
| `04bd746` | Add a test for HTTP error handling in the client. |
