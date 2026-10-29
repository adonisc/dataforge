# ADR-0006 — Mask on write into silver, with keyed HMAC

**Status:** Accepted · **Date:** 2026-10 · **Deciders:** Data Platform, Security

## Context

Several datasets contain PII. It has to be protected without losing the analytical value,
and the protection has to hold for copies, exports and backups too, not just the normal
query path.

## Decision

Mask **on write into silver**, based on each field's classification in the contract. The
default is **keyed HMAC-SHA256** with a versioned key, which gives a deterministic token
that can't be reversed. Raw values exist only in bronze.

## Alternatives considered

**Query-time masking (dynamic masking, secure views).** Good as an extra layer, weak on
its own. It's bypassed by querying the base table, copying to a sandbox, exporting to BI
or restoring a backup, and every access path has to stay configured correctly forever.

**Plain SHA-256.** Easy to reverse for emails: hash a list of known addresses and match.
A secret key prevents that.

**Reversible encryption (FPE or a token vault).** Lets you recover the original, which
support occasionally needs. Not the default because the recovery path is itself a risk,
and it makes GDPR erasure much harder. With HMAC, deleting the mapping does most of the
work.

**Redact everything.** Most protective, but analysts then ask for raw access, and once
that's granted the control is gone. `customers.yml` masks one email column two ways
(token + domain) so the masked data stays useful.

## Consequences

**Costs.** Key management is on the critical path: no key means no run, on purpose.
Rotating the key needs a planned re-tokenisation backfill. Tokens mean nothing to people,
so support tasks that need an email go through bronze, which is break-glass and audited.

**Benefits.** PII lives in one place with one retention policy and one access boundary.
Joins still work. Rotation is possible because every token carries its key version.
Masked columns are renamed (`customer_email` → `customer_email_token`) so nobody
mistakes a hash for an address.
