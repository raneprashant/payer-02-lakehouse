# CI/CD learning plan (implementation in later exercises)

Git flow: `feature/*` → `develop` (DEV), `release` (TEST), `main` (PROD).

Starter **already provides**:
- A Declarative Automation Bundle (`databricks.yml`) with target-to-catalog variables.
- One read-only `environment_check` job deployable to any target (when permissions/compute allow).
- Pull-request Python CI: lint and pure-Python tests; no Databricks credentials required.

Future exercises (NOT implemented yet):
1. Add bundle validation to CI with suitable credentials.
2. Automate DEV deployment on merge to `develop`.
3. Add TEST/PROD promotions with approvals and separate catalog settings.
4. Add integration tests, reconciliation gates, failure/rollback drills.
5. Replace local user profile in hosted CI with an approved CI credential, not local `.databrickscfg`.

Do not push secrets, host tokens, `.venv`, or `.databricks` state into Git.
