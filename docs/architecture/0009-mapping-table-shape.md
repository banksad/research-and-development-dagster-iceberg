# 0009 — Mapping table shape decision (lean refoundation)

## Status

Proposed.

## Context

The refoundation mapping slice now has a working connected path with explicit reference tables, contract-backed checks, and chain smoke coverage:

- `intermediate.staged_responses`
- `ref.ultfoc_mapper`
- `intermediate.mapped_responses`
- `ref.cell_number_mapper`
- `intermediate.cell_number_mapped_responses`

This has proven useful for incremental implementation and review. It also deliberately avoids lift-and-shift of legacy `run_mapping`; the refoundation path keeps mapping seams explicit, testable, and aligned to lean Dagster + Iceberg direction.

## Problem

If we continue adding mapping seams without deciding the long-term output shape, we risk creating an increasingly verbose and ambiguous intermediate layer, for example:

- `intermediate.mapped_responses`
- `intermediate.cell_number_mapped_responses`
- `intermediate.product_group_mapped_responses`
- `intermediate.geography_mapped_responses`
- `...`

That can produce graph/table sprawl, unclear canonical outputs, and downstream uncertainty about which mapped table should be consumed.

## Options considered

### Option A: Permanent seam-level output tables

Examples:

- `intermediate.foreign_ownership_mapped_responses`
- `intermediate.cell_number_mapped_responses`
- `intermediate.product_group_mapped_responses`

Pros:

- Very auditable seam-by-seam state.
- Easy to test each seam independently.
- Clear lineage between seam input and seam output.
- Easier debugging when a single seam regresses.

Cons:

- Creates many tables/assets over time.
- Downstream consumers must know the “final” mapping stage.
- Increases contract/check surface area.
- Can make the Dagster graph noisy.

### Option B: One canonical mapped output table

Example:

- `intermediate.mapped_responses`

Each mapping rule enriches the same conceptual mapped table.

Pros:

- Simple downstream contract.
- Closer to conceptual pipeline staging.
- Avoids table proliferation.
- Easier integration point for imputation/downstream assets.

Cons:

- Harder to inspect intermediate seams unless separately materialised.
- Seam-specific rule proof must be demonstrated via tests/checks rather than always-on seam tables.
- Mapping asset implementation can become too large if not modularised.

### Option C: Hybrid approach

Allow seam-level development/debug tables during refactoring, but define a single v1 canonical downstream mapping contract:

- `intermediate.mapped_responses`

In implementation, mapping business rules remain separated as pure domain functions by seam. Production-facing mapping materialisation composes selected rules into one canonical mapped table. Optional debug/checkpoint materialisations are retained only when justified.

Pros:

- Preserves seam-level business-logic modularity and testability.
- Gives downstream stages one stable canonical table.
- Avoids permanent graph/table sprawl.
- Keeps debugging support through targeted tests/checks and optional temporary materialisations.

Cons:

- Requires a later consolidation PR.
- Current `intermediate.cell_number_mapped_responses` may become development-only.
- Needs explicit naming/documentation discipline.

## Recommended decision

Recommend **Option C (Hybrid)** unless a strong delivery or assurance constraint requires a different shape.

Decision statements:

- Keep mapping business logic as seam-by-seam pure domain functions.
- Keep explicit reference tables (for example mapper refs) as first-class assets.
- Keep seam-specific tests/checks to prove rule behaviour.
- For v1 downstream consumption, define a single canonical mapping output table:
  - `intermediate.mapped_responses`
- Treat current `intermediate.cell_number_mapped_responses` as a development/checkpoint table unless later justified as production-facing.
- Before imputation consumes mapping outputs, make an explicit choice between:
  - temporarily consuming `intermediate.cell_number_mapped_responses`; or
  - consuming a consolidated `intermediate.mapped_responses` that includes foreign-ownership and cell-number mapping columns.

This decision explicitly does **not** reintroduce legacy `run_mapping` orchestration, legacy file-path plumbing, or wrapper-preservation as the target design.

## Consequences

Future mapping seam PRs should **not** automatically add permanent `intermediate.*_mapped_responses` tables.

Default implementation sequence should be:

1. Inventory the seam and contract intent.
2. Implement/refine pure domain seam logic.
3. Add explicit ref table(s) where needed.
4. Add seam-relevant tests/checks.
5. Then either:
   - compose the seam into canonical `intermediate.mapped_responses`; or
   - add a temporary seam table with explicit ADR/PR note describing why it is temporary and how consolidation will follow.

## Immediate next steps (post-ADR)

Recommended next direction:

1. **Next PR** should add a small mapping-consolidation/refactor plan that defines how foreign-ownership + cell-number seams converge into canonical `intermediate.mapped_responses` contract, without runtime code changes in that planning PR.
2. After plan agreement, decide whether `intermediate.cell_number_mapped_responses` remains as a justified debug/checkpoint asset or is deprecated once canonical consolidation lands.
3. Only then proceed with additional seams (for example product-group mapping) with explicit composition intent (canonical vs temporary seam table) recorded up front.

## Assumptions, risks, and rollback

Assumptions:

- Current seam-level assets/checks remain useful short-term migration slices.
- Downstream stages benefit from a single canonical mapping contract.

Risks:

- Consolidation sequencing could temporarily increase migration coordination effort.
- Ambiguity may persist if temporary seam tables are not clearly labelled in PRs/ADRs.

Rollback approach:

- This ADR is documentation-only and reversible by superseding ADR.
- If later evidence supports permanent seam-level outputs, publish a follow-up ADR that revises this recommendation before broadening seam rollout.
