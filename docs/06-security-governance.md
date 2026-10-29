# 06 — Security, privacy and governance

## Secure by default

The goal is that the insecure path doesn't exist at all, not just that it's discouraged.
Three places where that holds:

1. **No masking key, no run.** A contract with PII can't run without a key. There's no
   fallback to a plain hash and no passthrough. It's checked in `preflight`, before any
   data is touched.
2. **Tests enforce classification.** Every `pii`/`pci` field must be masked, and every
   masked field must be classified, so privacy audits can find it.
3. **Relaxing a classification is breaking under every compatibility policy**, including
   `none`, so it needs explicit approval.

---

## Classification

Classification is set on each **field** in the contract, not kept in a separate
spreadsheet where it's easy to forget.

| Class | Examples | Masking | Bronze retention |
|---|---|---|---|
| `public` | Currency codes, country codes | none | standard |
| `internal` | Order ids, statuses, amounts | none | standard |
| `confidential` | Margin, cost | role-gated | standard |
| `pii` | Email, name, phone, IP | required | **short** |
| `pci` | Card data | tokenise, never store | **shortest** |

Classifications are also written into the Parquet metadata and the OpenLineage schema
facet, so scanners like Macie or a catalog crawler can find sensitive columns without
access to this repo.

---

## Masking

### Where: on write into silver, not at query time

Query-time masking (views, dynamic masking, row access policies) is useful as an extra
layer, but on its own it can be bypassed by querying the base table, copying to a
sandbox, exporting to BI or restoring a backup.

Masking on write means raw values exist **only in bronze**, which gets short retention,
strict access and its own audit trail. One place to protect is manageable; every copy
ever made isn't.

### How: keyed HMAC, not a plain hash

Plain `sha256(email)` is easy to reverse: hash a list of known emails and match them. A
secret key held in the secrets manager blocks that.

Tokens are still **deterministic**, so `orders.customer_email_token` joins to
`customers.email_token`. You keep the analytics and lose the identity.

| Strategy | Output | Use when |
|---|---|---|
| `hash_hmac` | `v1:a3f8…` | The column is a join key. Default for identifiers |
| `email_domain_only` | `acme.co.uk` | The domain is useful, the person isn't |
| `last4` | `****4567` | People need to recognise a record without reading it |
| `redact` | `[REDACTED]` | No analytical value, e.g. IP addresses |

`customers.yml` masks the same email two ways: `email → email_token` and
`email → email_domain`. Offering useful masked versions means analysts are less likely
to ask for raw access.

IP addresses are redacted, not tokenised. An IP token has little value, and keeping a
joinable network identifier adds GDPR risk.

### Masked columns are renamed

`customer_email` becomes `customer_email_token`, so nobody mistakes a hash for an email
address.

### Key rotation

Every token includes its key version, so you can tell generations apart and scope a
re-tokenisation backfill. Without a version, rotating the key silently breaks every
historical join.

How to rotate: create `v2`; dual-write for a window; backfill silver from bronze with
`v2` tokens; check join counts match; retire `v1`.

---

## Least privilege

| Principal | Bronze | Silver | Gold | Quarantine |
|---|---|---|---|---|
| Ingestion task role | write own dataset | write own dataset | — | write |
| Transformation role | — | read | write | read |
| Analyst | — | read (masked only) | read | — |
| Data engineer | read (audited, break-glass) | read | read | read |
| BI service account | — | — | read (specific marts) | — |

Roles are per dataset and per layer, not one shared pipeline role. The `orders` task
can't read `customers` bronze, so a compromised task only exposes one dataset.

Human access to bronze is **break-glass and audited**, since that's the only place raw
PII lives.

---

## Encryption

- **At rest:** SSE-KMS with customer-managed keys, a separate key per classification
  tier, so key policy is a real access control.
- **In transit:** TLS everywhere, enforced by bucket policy (`aws:SecureTransport`).
- **Metadata DB:** encrypted; the Airflow Fernet key lives in the secrets backend.
- **Local Parquet:** encrypted at the volume level. The sample data is synthetic, which
  is why it can be committed.

---

## Environment separation

Separate **accounts**, buckets, catalogs, warehouses and IAM. Not schema prefixes in one
database, where one wrong variable sends a dev run to prod.

Production data never flows down. Dev and test use seeded synthetic data. To make it
realistic, the generator injects the kinds of defects production actually has, instead
of copying production into a less protected environment.

---

## Auditing

**Data access:** object-store access logs and warehouse query history, kept for a year,
with alerts on bronze PII reads and on large reads from PII tables.

**Pipeline changes:** every change is a git commit with an author and an approver. The
contract version is stored on every row, and the run manifest records contract version,
code version, inputs, metrics and outcome.

**Deployments:** the protected-environment approval is recorded against the commit.

**End-to-end trace:** gold row → `_run_id` → run manifest → contract version, code
version, source URIs and fingerprint → the exact bronze files, plus the Iceberg snapshot
from the day a report was produced. Each step is stored, not reconstructed.

---

## GDPR / right to erasure

Erasure is hard in an append-only lake, so it needs a plan:

| Layer | Approach |
|---|---|
| Bronze | Short retention does most of the work. Within the window, Iceberg row-level deletes |
| Silver | Already tokenised. Erasure removes the token-to-person mapping |
| Gold | Facts stay (the transaction happened); the dimension member is tombstoned |
| Backups | Covered by the retention window, which is why bronze retention is short |

Once the token-to-identity mapping is gone, the token isn't personal data in any
practical sense. That's a big reason HMAC is the default over reversible encryption.

The dbt test `assert_no_revenue_from_deleted_customers` catches new revenue for an
erased customer, which means the erasure didn't propagate or an id was reused.

---

## Threat model

| Threat | Control |
|---|---|
| Credential committed to git | gitleaks in CI and pre-commit; OIDC, so there's no static credential to commit |
| Over-broad IAM | Per-dataset, per-layer roles; quarterly access review |
| PII in logs | Redaction by key pattern in the log formatter |
| PII in error messages | Coercion errors include the value, so bronze-layer errors are treated as sensitive |
| Unmasked PII in silver | Hard failure without a key; contract tests; classification check in CI |
| Supply chain | Pinned base image, Airflow constraints file, Trivy, pinned dbt package ranges |
| Malicious contract change | Code review; CODEOWNERS on `contracts/`; relaxing classification is breaking |
| Compromised task | Short-lived scoped credentials; only one dataset exposed |
| Insider bulk export | Alerts on large PII reads in query history |
