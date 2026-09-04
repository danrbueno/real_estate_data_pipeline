# Update Notes

## Scope

This document records changes made since the creation of the `feature/scrapy-to-ai` branch, starting from commit base `0f299fb`.

The branch contains the implementation commits listed below. It also includes uncommitted local changes, identified in these notes as **In Progress**.

## Changes Since Latest Commit

The following changes are currently uncommitted after commit `336e2f2`:

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
- Shared configuration now checks `config/.env`, `app/.env`, and the project-root `.env`; `PROCESSED_DATA_DIR` is also exposed.
- Generated data files, scraped pages, and Python bytecode were removed from version control.

## Quality and Testing

- A deterministic AI Scraper test suite was added at `app/tests/test_ai_scraper.py`.
- Tests cover:
  - HTTP requests, rate limiting, and network errors;
  - OpenAI response parsing, including JSON, Markdown blocks, invalid responses, and exceptions;
  - extracted-data validation and property-detail extraction prompts;
  - HTML page counting and persistence;
  - pagination stop conditions;
  - CLI results and exit codes.
- Pytest configuration is located at `app/pytest.ini`.
- Coverage configuration is located at `app/.coveragerc`, with branch coverage and a 100% minimum for `app.ai_scraper`.
- Current focused validation command:

```powershell
python -m pytest --no-cov -q app/tests/test_ai_scraper.py
```

- Latest local result: 36 passing tests with 100.00% line and branch coverage for `app.ai_scraper`, including both downloader agents and their CLIs.

## CI and Release

- The `.github/workflows/release-quality.yml` workflow was added.
- The workflow installs dependencies and runs `python -m pytest -c app/pytest.ini` for pull requests and pushes to `main`.
- The `.github/skills/deploy-full-coverage/SKILL.md` skill was added to define the full-coverage and release-validation workflow.
- The `.github/agents/release-manager.agent.md` agent was added to apply the skill, issue release decisions, and require explicit approval before a production deployment.

## Documentation

- The root README was updated to reflect the AI Scraper architecture.
- Architecture, quick-start, project-structure, optimization, and checklist guides were added under `docs/`.
- Historical Scrapy references were removed from active documentation and code comments.

## In Progress and Limitations

- The 100% coverage requirement currently applies only to `app.ai_scraper`.
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
