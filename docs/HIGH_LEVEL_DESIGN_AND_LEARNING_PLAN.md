# Payer 02 Lakehouse — High-Level Design & Learning Plan

**Document type:** Architecture orientation and development roadmap  
**Applies to:** `payer-02-lakehouse` starter repository **v0.0.1** and `payer-02-data-generator` **v0.1.1**  
**Status:** Learning plan / high-level design. The Bronze, Silver, and Gold business pipelines are **not yet implemented**.  
**Primary developer:** You. **Architect/reviewer/mentor:** ChatGPT.  
**Working style:** Design requirements first; you implement code; review and targeted guidance when needed. No finished transformation code by default.

## 1. What we are building

A healthcare-payer enterprise Lakehouse with separate Bronze Snapshot and CDC feeds, effective-dated Silver business entities, Gold analytical products, and CI/CD promotion using Databricks Declarative Automation Bundles (DABs).

```text
payer-02-data-generator (separate repository)
  └── MEDIUM: B0001 Snapshot → B0002 ... B0007 CDC/events
          │  Upload one batch at a time (not all at once)
          ▼
Databricks Unity Catalog Volume
  payer_02_dev.landing.raw_files/landing/<entity>/
          │  Auto Loader; explicit schema and source-file selection
          ▼
BRONZE (raw Delta, append-only)
  <entity>_snapshot and <entity>_cdc (event-only entities use _events)
  + source columns + ingestion metadata; independent checkpoints
          │  DQ, source sequence, CDC, corrections, timelines, joins
          ▼
SILVER (validated, conformed business state and history)
  members, subscriptions, coverage/enrollment, provider/network,
  addresses, claims, adjudication, finance and audit/exceptions
          │  incremental dependencies and historical restatement
          ▼
GOLD (analytics-ready dimensional and business products)
  member-months, claim-line facts, payments, PMPM, denial rates,
  financial reconciliation, dashboards
          ▲
Git / GitHub CI + Databricks Bundles + DEV → TEST → PROD deployment
```

**Workspaces and environments:** One Databricks Free Edition workspace; separate Unity Catalog catalogs `payer_02_dev`, `payer_02_test`, `payer_02_prod`. Hands-on development and debugging happen **only in `payer_02_dev`**. `DATABRICKS_FREE` is the CLI **profile**. `SMOKE`/`MEDIUM`/`LARGE` are generator **Data Scales**. `dev`/`test`/`prod` are bundle **targets**.

**Source QA is separate:** `payer_02_validation.source_qa` was used to approve generated data; it is not the source of our production-style Bronze pipeline. We ingest the **original files** into the DEV landing volume.

## 2. Core architecture rules we will practice

| Concern | Architectural rule |
|---|---|
| Input arrival | Flat per-entity directories with batch ID + arrival date in filename; upload batches individually to observe change. |
| Bronze | Snapshot and CDC go into **separate append-only Delta tables**; event-only feeds have an `_events` table. Preserve source fields; add technical ingestion metadata, no business correction. |
| Auto Loader | Use a schema, file-type-specific selection, a dedicated checkpoint per feed, and safe re-execution. |
| CDC | Order by **source event sequence/version**, not arrival date, file name or Auto Loader discovery order. Handle duplicates, rare `D` source retractions, and late/out-of-order records. |
| Silver | Define row grain and key, validate business rules, quarantine invalid data, reconstruct current state/effective-dated history; test idempotency and restatements. |
| Eligibility | At most one active Coverage per member on a business date; continuous, non-overlapping Enrollment segments inside each Coverage; four mandatory dates. Address-only change does not create an Enrollment. |
| Provider and address | Practitioner → Role/Affiliation → exact Supplier Location → Supplier; network and contract effective dates; shared Address Master and effective-dated Entity-Address Bridge. |
| Finance | Claim `CLOSED` is **not** proof of payment; derive payment status from remittance allocations, adjustments and recoveries. |
| Gold | Explicit grain and reporting date basis; recompute affected historical slices when older business events change. |
| CI/CD | Same code, target-specific catalog; test and review before promotion. No secrets in Git. |

## 3. Starter repository — what every component does

The explanations below match the **actual files in `payer-02-lakehouse-starter-v0.0.1.zip`**. Every row gives the component's purpose and why we need it. Files marked *exercise* are deliberately unfinished.

### A. Project and developer configuration

| File | Purpose / why it exists |
|---|---|
| `README.md` | Entry point explaining what the project is and what is intentionally incomplete. Read it when returning to the project after a break. |
| `CMD_README.md` | Copy-ready Windows PowerShell, Databricks CLI and Git commands. Reference it rather than trying to remember syntax. |
| `LEARNING_NOTES.md` | Your engineering journal: design, expected results, actual results, failures and solutions. Reinforces independent recall. |
| `pyproject.toml` | Python package metadata and local development dependencies (`pytest`, `ruff`); enables editable installation. Local tests don't require Spark. |
| `.gitignore` | Excludes virtual environments, caches, output, local Databricks deployment state and secrets from commits. |
| `.vscode/settings.json` | Selects the project venv and configures Python analysis and test discovery in VS Code. |
| `.vscode/extensions.json` | Suggests useful VS Code extensions (Python, Databricks, YAML); does not automatically install them. |

### B. Deployment and environment management

| File | Purpose / why it exists |
|---|---|
| `databricks.yml` | **Top-level Databricks Bundle configuration**: shared resources and target-to-catalog mapping for DEV/TEST/PROD. Later all pipeline jobs will be deployed using this configuration. |
| `resources/00_environment_check.job.yml` | First small deployable job definition. Proves bundle wiring without changing business tables; later add Bronze/Silver/Gold job definitions under `resources/`. |
| `src/notebooks/00_environment_check.py` | Read-only Databricks notebook verifying target/catalog alignment and schema visibility. Use it to validate a deployment before running actual loads. |
| `.github/workflows/python_ci.yml` | Existing GitHub Actions CI: lint and pure-Python tests for PRs/pushes. We will extend CI and add authenticated CD later; no credentials are needed for its current checks. |

### C. Shared Python framework and pipeline entry points

| File | Purpose / why it exists |
|---|---|
| `src/payer_lakehouse/__init__.py` | Marks the reusable Python package. Common tested helpers will live in this package, not be copied into every notebook. |
| `src/payer_lakehouse/paths.py` | Centralizes approved Unity Catalog volume paths, Bronze table names and checkpoint conventions. Guards against accidental target/catalog mismatch. |
| `src/notebooks/10_bronze_member_snapshot.py` **(exercise)** | First PySpark Auto Loader pipeline: ingest `member_master_snapshot` Parquet to append-only Bronze with audit columns and checkpointing. TODOs are intentionally yours to implement. |
| `src/notebooks/20_silver_member.py` **(exercise)** | Second pipeline: validate Bronze Member Snapshot and populate a member-grain Silver table with defined data-quality handling. CDC is added in a later lesson, not now. |

**Development expectation:** As we expand to more source entities, you will design new notebooks/Python modules, shared helpers and job resource files. They are **not** present yet and should not be mistaken for delivered functionality.

### D. SQL setup and acceptance checks

| File | Purpose / why it exists |
|---|---|
| `sql/00_setup_dev_catalog.sql` | Creates DEV schemas (`landing`, `bronze`, `silver`, `gold`, `control`) and the landing/checkpoint Volumes. Run once before uploading B0001 to DEV. |
| `sql/01_member_snapshot_checks.sql` | Bronze acceptance queries: row count, potential duplicate business keys and ingestion audit fields. Run before/after a rerun to prove checkpoint behavior. |
| `sql/02_member_silver_checks.sql` | Silver acceptance queries: member count, key uniqueness, null keys and source status distribution. These validate your code; they do not create the table. |

### E. Exercises, reference design and local tools

| File | Purpose / why it exists |
|---|---|
| `docs/00_START_HERE.md` | Minimum ordered onboarding checklist, from local tests to DEV environment setup and B0001 upload. Follow before starting Exercise 01. |
| `docs/ARCHITECTURE_AND_DELIVERY.md` | Record of approved architectural conventions, environment separation, storage layout, history and CDC requirements. Used as the ground rules for design reviews. |
| `docs/CI_CD_PLAN.md` | Explains what CI/CD exists now and what you'll implement later (DEV deployment, TEST/PROD promotions, gates and rollback drills). |
| `docs/SOURCE_CONTRACTS_REFERENCE.json` | Copy of the generator's 29 source contracts, formats, keys and dependency order. Your primary reference when defining schemas and source-to-target mappings. |
| `docs/MAPPING_TEMPLATE.md` | Repeatable template for mapping each source/feed to its target: grain, keys, transformations, DQ rules, restart behavior and tests. Fill one in before coding each pipeline. |
| `exercises/EX01_BRONZE_MEMBER_SNAPSHOT.md` | First guided assignment and explicit pass/fail criteria for Auto Loader Member Snapshot ingestion. |
| `exercises/EX02_SILVER_MEMBER.md` | Second assignment to design and implement Member Snapshot → Silver; deliberately leaves the logic to you. |
| `tools/check_contracts.py` | Lightweight command to inspect contract entities and the generator's 12-step order; helps verify assumptions without a Spark environment. |
| `tools/upload_batch.ps1` | Uploads **only one requested batch** (B0001, then B0002, etc.) into the DEV landing Volume, preserving relative entity folders. Prevents accidentally exposing all CDC batches at once. |

### F. Tests

| File | Purpose / why it exists |
|---|---|
| `tests/test_paths.py` | Tests catalog-to-target routing, path construction, valid identifiers and separate Snapshot/CDC checkpoints. Protects deployment conventions. |
| `tests/test_contract_reference.py` | Verifies the 29 copied contracts, 12 generator steps, mandatory Enrollment dates and effective-dated Address Bridge columns. Protects against contract drift. |

## 4. How we'll develop each new pipeline

**Architect provides first:** (1) business requirement, (2) source/target contracts, (3) target grain and keys, (4) logical processing design, (5) dependency and CDC strategy, (6) expected test cases, (7) acceptance criteria. For larger components, this becomes a small, separate **low-level design (LLD)** document before coding.

**You then:** draw the flow, complete `docs/MAPPING_TEMPLATE.md`, code the notebook/module in VS Code, run tests and pipeline, compare actual and expected results, explain any discrepancy, and commit the solution. **Ask questions freely** about SQL/PySpark/Databricks commands or concepts; I will answer those without giving away the entire implementation unless asked.

**Code review checklist:** correct grain/key; correct time semantics; preserving raw Bronze; deterministic CDC replay; restart safety; DQ quarantine and counts; performance and readability; target parameterization; tests; Git/CI/CD deployment safety.

**Suggested development order:**

1. **Foundation:** local tests, DEV schemas/Volumes, target-aware paths, environment-check job, and Git branches.
2. **Exercise 01:** Member Snapshot → Bronze via Auto Loader; stable checkpoint and exact row reconciliation.
3. **Exercise 02:** Member Snapshot → Silver; validation/quarantine and key uniqueness.
4. **Reusable Bronze ingestion:** Snapshot/CDC/event-only variants, schema handling and independent checkpoints; test with multiple domains.
5. **Silver reference + relationship domains:** Product/Benefit Plan/Group/Offering; Address; Supplier/Location; Practitioner/Role; Network/Contract.
6. **Complex Silver history:** Subscription/Family; Coverage and Enrollment continuity; CDC/MERGE, SCD 1/2, retroactive and delayed events.
7. **Claims and finance:** header/line/status/adjustment history; service-date eligibility/provider joins; payment and reconciliation logic.
8. **Gold:** member-month fact, claim-line fact, payment fact, PMPM, denials, financial aggregates and selective restatements.
9. **CI/CD + hardening:** parameterized jobs/bundles, unit/integration tests, `develop` → DEV, `release` → TEST, `main` → PROD, failure recovery and performance exercises.

You do **not** need to finish all Bronze tables before learning any Silver. We will use complete vertical slices, then generalize reusable patterns to other entities.

## 5. First vertical slice — high-level design only

**Input:** B0001 `member_master_snapshot_*.parquet` in `payer_02_dev.landing.raw_files/landing/member_master/`.  
**Bronze output:** `payer_02_dev.bronze.member_master_snapshot`; each source row is preserved with ingestion metadata.  
**Execution:** Auto Loader triggered processing; dedicated checkpoint at `payer_02_dev.control.checkpoints/...`.

**Expected tests:** (a) row count matches the B0001 manifest; (b) raw columns preserved; (c) audit fields populated; (d) rerun with no new files does not add rows. Once this works, define a Member Silver mapping with one row per `member_id` and explicit invalid-row handling.

**Not yet implemented:** Actual Auto Loader code, Silver transformations, other domain pipelines, Gold, most Jobs, and deployment automation. You are responsible for writing these as you progress.

## 6. Should you read the generator first?

**Yes, but only a focused architectural reading — not a full line-by-line study.** You already generated and verified the source data. Spend **about 6–10 hours (3–5 two-hour sessions)** understanding the major components, then move to Lakehouse development.

| Read in this order | What to learn | Approx. time |
|---|---|---|
| `src/payer_generator/cli.py` and `__main__.py` | How a terminal command selects modes, dates, Data Scale and scenarios. Trace one command from the CLI. | 1–2 h |
| `src/payer_generator/contracts.py` and `docs/SOURCE_CONTRACTS_v2.json` | Source schemas, business keys, file formats and 12-step dependencies. Focus on Member, Coverage, Enrollment, Claims. | 1–2 h |
| `src/payer_generator/storage.py` | How SQLite stores state, how writers emit CSV/JSON/Parquet, how batches and manifests commit. | 2 h |
| `src/payer_generator/generator.py` and `scenarios.py` | Read `snapshot()`, `eligibility()`, `split_enrollment()`, `scenario_pcp_change()` and `scenario_late_arrival()`; trace one change across entities. | 2–3 h |
| `src/payer_generator/validate.py` and `tests/` | How generated business rules are checked and regression-tested. | 1 h |

**One useful reading exercise:** Trace a PCP change end-to-end: CLI selection → scenario → updated Enrollment row(s) → `Batch.emit_change` → CDC file → SQLite state and manifest. Explain the three clocks: file arrival, source event, business effective date. **Do not spend time rewriting the generator** unless a defect blocks Lakehouse development.

## 7. Practical schedule at two hours per day

These are **learning-time estimates, not deadlines**. Actual duration depends on how many of the 29 entities and advanced scenarios you implement independently.

| Learning milestone | Approx. focused hours | Approx. two-hour sessions |
|---|---:|---:|
| Generator architecture reading | 6–10 | 3–5 |
| Starter setup + first Bronze/Silver vertical slice | 16–28 | 8–14 |
| Reusable Bronze patterns and main source domains | 30–55 | 15–28 |
| Advanced Silver CDC, temporal joins and reconciliation | 65–110 | 33–55 |
| Gold data products and historical restatements | 35–65 | 18–33 |
| CI/CD, operational recovery and independent capstone | 30–55 | 15–28 |

A **solid working implementation of the core patterns** might take roughly **3–5 months** at two hours most days. Building out **nearly every proposed entity, Gold product and advanced operational exercise** could take **6–9+ months**. Prioritize independent skill, not a calendar deadline.

**A two-hour session routine:** 15 min recall yesterday's concept and design; 70 min implement/debug without copying; 25 min test/reconcile; 10 min write learning notes and Git commit. Once a week, rebuild one familiar transformation from memory.

## 8. How we'll know you're becoming independent

- **Foundation:** You can implement Snapshot Auto Loader and test idempotency without a provided solution.
- **Intermediate:** You can design and implement CDC/SCD processing from a mapping specification and validate unusual batches.
- **Advanced:** You can independently diagnose retroactive corrections, historical Gold changes, reprocessing and payment reconciliation.
- **Architect:** You can select grains/keys, explain trade-offs, design target-aware bundles and review another developer's implementation.

**Next action:** Add this file to `docs/HIGH_LEVEL_DESIGN_AND_LEARNING_PLAN.md`. Read the Starter component guide and spend a few sessions on the generator code-reading exercise. Then begin Exercise 01, with me providing its detailed design and acceptance tests **before** you write the implementation.
