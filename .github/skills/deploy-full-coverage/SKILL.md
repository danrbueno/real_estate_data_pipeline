---
name: deploy-full-coverage
description: 'Deploy an application only after achieving and verifying 100% automated test coverage. Use when preparing a release, configuring coverage gates, adding missing tests, validating CI/CD deployment readiness, or enforcing a full-coverage policy.'
argument-hint: 'Describe the application, deployment target, and coverage tool if known'
user-invocable: true
disable-model-invocation: false
---

# Deploy With Full Coverage

## Outcome

Produce a deployable application only when its automated test suite passes and the project's configured coverage metric is exactly 100%. Make the coverage scope explicit, validate the production deployment path, and leave reproducible evidence of both checks.

## Repository Profile

This workspace is a Python real-estate data pipeline. Its application code is in `app/ai_scraper/`; Airflow DAGs and pipeline modules are in `airflow/dags/`. Dependencies are declared in `config/requirements.txt`, which includes `pytest` but no coverage tool. No CI workflow, deployment manifest, or production deployment command is currently defined.

Run commands from the repository root. Use `python -m pytest`, so the selected Python environment is unambiguous. Before running imports or tests that refer to `ai_scraper`, set `PYTHONPATH=app` in the active shell or invoke Python with the `app` directory available on its import path.

## Scope And Policy

1. Identify the deployment target, release command, CI provider, coverage tool, and the testable source scope.
2. Treat "100% coverage" as 100% of the coverage metric enforced by the project. Prefer branch coverage when it is already configured; otherwise use the existing line coverage configuration.
3. Exclude only generated code, vendored dependencies, framework bootstrap code, or unreachable defensive code when the repository's policy explicitly permits it. Record every exclusion and its rationale.
4. Do not weaken thresholds, remove tests, add blanket exclusions, or mark failures as allowed merely to make the gate pass.

## Procedure

1. Inspect `app/ai_scraper/`, `airflow/dags/`, `config/requirements.txt`, existing tests, coverage settings, deployment documentation, and CI/CD workflow. Reuse the repository's package manager, test runner, formatter, type checker, and release conventions.
2. If coverage tooling is absent, add and pin `pytest-cov` in `config/requirements.txt`. Add a focused `pytest.ini`, `pyproject.toml`, or `.coveragerc` configuration that measures the agreed source scope and sets `--cov-fail-under=100`; do not add broad omissions.
3. Establish a baseline with `python -m pytest --cov=app/ai_scraper --cov=airflow/dags --cov-branch --cov-report=term-missing --cov-fail-under=100`. Capture failing tests, uncovered files and lines, coverage metric, and current threshold. Adjust module paths only when the project test configuration requires it.
4. Convert each uncovered behavior into a testable requirement. Add focused tests for normal behavior, boundary conditions, error handling, and branches that affect the scraper, data transformation, database loading, or Airflow task behavior.
5. Rerun the focused tests after each change. Fix production defects at their source; do not encode accidental behavior solely to satisfy coverage.
6. Run the full test suite with coverage. Continue until the configured target is exactly 100% and no unauthorized exclusions were introduced.
7. Run the project's static checks required for release, such as formatting, linting, type checking, security scanning, packaging, or build verification. Fix only issues caused by the deployment work unless the release policy requires otherwise.
8. Validate the production-equivalent path: execute the scraper against isolated fixtures, validate generated JSON and CSV data, import and test the Airflow DAG, and exercise database behavior against an isolated test database. Never call OpenAI or the live scraped website from automated tests.
9. Before deployment, obtain the production target, release command, secret-management mechanism, migration procedure, monitoring destination, and rollback plan. If these are not defined, stop and report the missing deployment contract instead of inventing one.
10. Configure or confirm CI/CD enforcement so that deployment jobs depend on successful tests, the 100% coverage report, and release checks. Ensure the coverage report is uploaded or retained as build evidence.
11. Deploy using the repository's approved command or pipeline. Do not expose secrets in commands, logs, source files, or reports.
12. After deployment, run a scoped smoke test, verify the Airflow DAG is visible and healthy, observe errors or key metrics for the agreed window, and confirm rollback readiness.

## Decision Points

- If the coverage command is absent or ambiguous, inspect project tooling and ask before introducing a new coverage framework.
- If a source file is intentionally excluded, require a narrow, documented exclusion approved by project policy; otherwise test it.
- If external services block deterministic tests, use the project's existing fakes, fixtures, record/replay tooling, or isolated test environment. Do not rely on live production services.
- If 100% is infeasible due to generated or runtime-only code, stop before deploying and report the exact blockers, proposed policy exception, and residual risk.
- If tests or checks fail after deployment preparation, do not deploy. Repair, rerun the relevant checks, and repeat the full release gate.
- If deployment verification fails, roll back or stop promotion according to the existing release process, then preserve diagnostic evidence.

## Completion Criteria

Deployment is complete only when all of the following are true:

- The full test suite passes.
- The enforced coverage metric is exactly 100% for the agreed source scope.
- Coverage exclusions are minimal, explicit, and policy-compliant.
- Required build, lint, type, security, and packaging checks pass.
- CI/CD prevents deployment when tests or coverage fail.
- The production artifact was deployed and passed its health and smoke checks.
- The deployed version, coverage result, verification result, and rollback procedure are documented in the release record.

## Final Report

Report the commands run, coverage metric and scope, final coverage result, added or changed tests, approved exclusions, artifact/version deployed, smoke-test result, CI gate status, and any remaining monitoring or rollback actions.
