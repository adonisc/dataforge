# 05 — CI/CD and delivery

The pipeline is defined in `.github/workflows/ci.yml` (GitHub Actions).

---

## Stage order

```
lint ──► contracts ──► test ──► data-quality ──► build ──► deploy-dev ──► [GATE] ──► prod
 20s       30s          25s         3min          4min        1min       human      2min
```

**Cheap checks first, so failures show up in seconds.** Lint before tests, tests before
the image build, the build before anything that touches a cloud account.

---

## What each stage does

### 1. Lint, format, strict typing, secret scan

`ruff format --check`, `ruff check`, `mypy` in strict mode, `gitleaks`.

Strict typing **blocks the merge**. Every pipeline imports this framework, so a type
error here becomes a runtime error everywhere.

A committed secret should be treated as leaked and rotated. The scan makes sure that
happens now and not at the next audit.

### 2. Contract checks

**`dataforge validate-contracts`**: every contract parses and every rule type exists.
With `extra="forbid"`, a typo in a key or rule type fails the merge request instead of
turning into a rule that never runs.

**`dataforge check-compatibility --base <target-branch>`**: catches schema changes that
would break consumers, before merge. Breaking changes are allowed, but a dropped column,
a narrowed type, a changed grain or a relaxed PII classification needs a major version
bump.

### 3. Unit and integration tests

The suite runs in seconds. Coverage has a **floor, not a target**: the floor stops it
slipping, while a target tends to produce tests that just touch lines.

### 4. Data quality in CI

Generate synthetic data with **known defects**, run the real pipeline over three dates,
then `dbt build`. Unit tests check each rule; this checks the whole thing still
quarantines and blocks what it should.

Use `dbt build`, not `dbt run && dbt test`. Build runs each model's tests before building
anything on top of it.

### 5. Build and scan

Images are tagged by **commit SHA, never `latest`**, so you can roll back and always know
which code produced a number. Trivy fails the build on CRITICAL or HIGH CVEs.

### 6–7. Deploy dev, approval, prod

Dev deploys automatically. Prod sits behind a protected environment with required
reviewers: it can't start until someone approves, and the approval is recorded against
the commit.

**Why a manual gate after all the checks pass?** The checks show the code is correct.
They can't tell whether *now* is a good time (quarter close, an ongoing incident, the
Friday before a holiday).

The gate only appears after lint, typing, secret scan, tests, contracts, the
compatibility check, the synthetic pipeline run, `dbt build` and the CVE scan have all
passed.

---

## What blocks promotion

**Blocking:** lint, format or type errors; a detected secret; an invalid contract; a
breaking schema change without a major bump; failing tests or coverage below the floor;
a blocking quality rule firing on the fixtures; a failing `severity: error` dbt test; a
CRITICAL/HIGH CVE.

**Non-blocking:** `severity: warn` dbt tests; source-freshness warnings in CI (only
meaningful in production); coverage changes above the floor.

The split follows a simple rule. A check blocks when missing a problem costs more than a
false alarm. Making warnings blocking would keep the pipeline red, and people would start
ignoring red.

---

## Merge requests vs the default branch

| | Merge request | Default branch |
|---|---|---|
| Lint, types, tests | Yes | Yes |
| Compatibility vs base | **Yes** | Skipped (nothing to compare) |
| Pipeline smoke + dbt build | Yes | Yes |
| Image build/push | Build only | Build and push |
| Deploy | No | Dev automatic, prod gated |

---

## Environment configuration

**The same image runs in dev, test and prod.** Only the injected environment
(`PipelineConfig`, dbt `target`) and the IAM role differ.

Because the binary is identical, a difference in behaviour comes down to data or
permissions, which makes problems much quicker to track down.

All config lives in one explicit object, not in `os.environ` reads spread through the
code, so it can be reviewed in one place.

Environments are separated by **database**, not schema prefix, so a wrong variable can't
point a dev run at prod.

---

## Secrets

1. **No secrets in the repo, the image or long-lived CI variables.** CI uses OIDC to get
   a short-lived role, so there's no static key to leak or rotate.
2. **App secrets** (masking key, API tokens) are read from the secrets backend at task
   start, scoped to the task's role, and never logged. The logger also redacts keys
   matching `pass|pwd|secret|token|api_key|authorization|credential`.
3. **Rotation is built in.** Every HMAC token carries its key version, so old and new
   tokens can coexist during a re-tokenisation backfill.

Contracts reference secrets by URI (`secret://platform/fx-api/readonly`) and never hold
the values.

---

## Rollback

| What broke | Rollback |
|---|---|
| DAG / pipeline code | Point the Git DAG bundle back at the previous SHA. Running jobs keep their code |
| dbt model | Revert the commit; `dbt build --select <model>+` to rebuild it and its children |
| Bad data published | Reprocess from **bronze** with `--force` for the affected dates |
| Bad data already used downstream | Iceberg time travel to the snapshot before the incident, then reprocess |
| Contract change | `git revert` |

A deploy isn't done until the next run succeeds. The prod job should end with a canary
run and a freshness check (a placeholder for now).
