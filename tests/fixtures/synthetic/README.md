# Synthetic fixture harness

These fixtures are **synthetic**, intentionally tiny, non-sensitive datasets committed to the repository for migration safety scaffolding.

## Purpose

- Each fixture must exist to prove a specific business rule, transformation seam, or parity comparison point during the refoundation migration.
- Fixtures should be directly readable and testable by humans (small CSV + scenario metadata).
- Fixtures are intended to support legacy-vs-refoundation output checks as pathways are migrated.

## Constraints

- Do not include real data, production extracts, secrets, credentials, or environment-specific paths.
- Do not mimic production volume; keep scenarios very small.
- Keep row-level intent explicit so reconciliation failures are understandable.

## Usage pattern for migration PRs

1. Add or extend a scenario only when needed to validate a migration seam.
2. Run legacy-path logic and refoundation-path logic against the same synthetic input fixture.
3. Compare outputs at a declared grain and document parity results in the PR.
4. Treat differences as either explicitly expected or defects requiring follow-up.

Real data must never be committed to this fixture area.
