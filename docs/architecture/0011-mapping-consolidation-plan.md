# 0011 — Mapping Consolidation Plan

## Status

Implemented for v1 canonical mapping output (follow-up deprecation of checkpoint asset pending).

## Purpose

This plan defines how the current mapping implementation shape (which is intentionally seam-by-seam and checkpoint-friendly) should converge into a production-facing canonical mapping output.

It bridges the current development/checkpoint shape to the target production mapping shape by clarifying semantics, sequencing, and migration guardrails.

Specifically:
- current seam tables have been useful for development, debugging, and testing;
- production users need a clean, stage-level DAG that is easy to operate and explain;
- downstream stages should consume one canonical mapped table;
- this document is a planning artifact only and does **not** implement consolidation.

## Current state

The currently implemented mapping slice is:

- `intermediate.staged_responses`
- `ref.ultfoc_mapper`
- `intermediate.mapped_responses`
- `ref.cell_number_mapper`
- `intermediate.cell_number_mapped_responses`

Current meanings:

- `intermediate.staged_responses`: staged response input to mapping.
- `ref.ultfoc_mapper`: explicit reference input for foreign-ownership mapping.
- `intermediate.mapped_responses`: currently means **foreign-ownership mapped** responses.
- `ref.cell_number_mapper`: explicit reference input for cell-number mapping.
- `intermediate.cell_number_mapped_responses`: currently means **foreign-ownership + cell-number mapped** responses.

This seam-by-seam shape has been useful as a migration checkpoint and for isolated validation, but it is not the desired long-term production-facing naming/consumption model.

## Target production shape

`intermediate.mapped_responses` should eventually mean:

> fully mapped responses for the currently implemented production mapping scope.

For the current v1 mapping scope, that means:
- foreign ownership mapping applied;
- cell-number mapping applied;
- relevant mapper/reference-derived columns included;
- required mapping checks attached to the canonical mapped output.

Conceptual DAG target:

```text
intermediate/staged_responses
  + ref/ultfoc_mapper
  + ref/cell_number_mapper
  -> intermediate/mapped_responses
```

Optional debug/checkpoint outputs may exist where justified for development and troubleshooting, but they should not be required in the production-facing path consumed by downstream stages.

## Consolidation options

### Option A — Expand existing `mapped_responses` to apply both seams sequentially

Modify the existing `mapped_responses` asset behavior so it applies foreign-ownership mapping and then cell-number mapping in sequence, writing canonical `intermediate.mapped_responses`.

**Pros**
- Simplest production DAG.
- Preserves one canonical mapped output.
- Downstream stages consume a stable, semantically clear table name.

**Cons**
- Asset function can grow unless implementation delegates to small domain functions.
- Migration needs careful tests/checks so foreign-ownership assurances are not regressed.

### Option B — Introduce a temporary new canonical asset name and map it to canonical table output

Add a new canonical-named asset (for example `canonical_mapped_responses`) that writes `intermediate.mapped_responses`, while deprecating foreign-ownership-only behavior currently associated with `mapped_responses`.

**Pros**
- Explicit transition boundary.
- Can be easier to review in narrowly scoped migration PRs.

**Cons**
- Awkward naming and temporary duplication.
- Can confuse short-term asset/table ownership semantics.
- Risks duplicated responsibilities during overlap.

### Option C — Keep seam tables and have downstream consume `intermediate.cell_number_mapped_responses`

Retain current seam-by-seam production consumption by pointing downstream stages to `intermediate.cell_number_mapped_responses`.

**Pros**
- Least immediate code change.

**Cons**
- Violates the production-facing canonical-table direction.
- Encourages graph/table sprawl.
- Makes downstream naming depend on whichever mapping seam happens to be latest.

## Recommendation

Adopt **Option A** with strict internal modularity.

Recommended direction:
- `mapped_responses` becomes the canonical production-facing mapping asset.
- It composes seam-level domain functions internally (e.g. `apply_foreign_ownership_mapping(...)`, `apply_cell_number_mapping(...)`, and future seam functions).
- It reads explicit mapping reference inputs (`ref.ultfoc_mapper`, `ref.cell_number_mapper`, plus future mapper refs).
- It writes canonical `intermediate.mapped_responses`.
- `intermediate.cell_number_mapped_responses` is treated as a temporary development/checkpoint output and is eventually deprecated or made optional/debug-only.

This preserves readable production orchestration while retaining seam-level logic separability.

## Checks strategy

After consolidation, checks should be migrated/composed so canonical `intermediate.mapped_responses` carries mapping assurance for implemented scope, including:

- table exists;
- non-empty;
- required mapped-response contract columns present;
- unique grain on `reference + instance + survey_type + survey_year`;
- `ultfoc` populated/defaulted as required;
- cell-number metadata populated where `cellno` is non-null;
- no unexpected unmapped mandatory references;
- future mapper coverage checks as additional seams are implemented.

Reference-table checks remain attached to:
- `ref.ultfoc_mapper`;
- `ref.cell_number_mapper`.

If `intermediate.cell_number_mapped_responses` remains during transition, its checks should be clearly labeled development/checkpoint checks, not production-facing canonical checks.

## Table contract strategy

`config/table_contracts/base.yaml` should eventually be updated so the contract for canonical `intermediate.mapped_responses` reflects the implemented mapping scope.

For the current scope, expected required fields include:

- `survey_year`
- `survey_type`
- `reference`
- `instance`
- `ultfoc`
- `cellno`
- `cellnumber`
- `uni_count`
- `uni_employment`

`intermediate.cell_number_mapped_responses` contract should eventually be either:
- deprecated;
- explicitly marked development/debug;
- or removed only once no longer used.

This planning PR does **not** implement those contract changes.

## Implementation-transition note

- The canonical `intermediate.mapped_responses` table contract has now been updated ahead of runtime consolidation implementation.
- This intentionally creates a short transition period where the contract documents the target v1 semantics while runtime behavior still reflects the current seam-by-seam implementation.
- Consolidation implementation has now landed: `mapped_responses` composes foreign-ownership and cell-number seams and satisfies the expanded canonical contract.
- Remaining follow-up: mark and later deprecate `intermediate.cell_number_mapped_responses` as a checkpoint/debug asset.

## Test strategy

Future implementation PRs should follow a migration path that preserves confidence while changing canonical semantics:

- keep domain tests for foreign-ownership and cell-number seams;
- keep mapper ref-table tests/checks;
- update asset-level tests so canonical `mapped_responses` assertions include both foreign-ownership and cell-number mapped output;
- update chain smoke coverage so materialization path includes:
  - `staged_responses`
  - `ultfoc_mapper`
  - `cell_number_mapper`
  - `mapped_responses`
- verify canonical `intermediate.mapped_responses` matches expected fully mapped outputs;
- preserve no-legacy-import guardrails;
- do not remove old checkpoint tests until replacement canonical tests are passing.

## Operator / production implications

This consolidation direction improves production operability by providing:

- cleaner, stage-level Dagster DAG readability;
- one mapped table to inspect before imputation;
- fewer confusing intermediate mapping outputs in production runs;
- mapping assurance points still visible through checks;
- simpler Launchpad/run monitoring;
- clearer explanation path for statisticians and production support.

## Reproducibility implications

Consolidation should not reduce reproducibility.

Seam-level lineage and assurance are preserved through:
- explicit mapper reference-table snapshots;
- Dagster run metadata;
- asset checks;
- seam-level domain tests;
- optional debug/checkpoint materializations where justified.

Iceberg snapshot history on canonical `intermediate.mapped_responses` continues to record production output state over time.

## Proposed PR sequence

After this planning PR, implement in small, reversible steps:

- **PR A:** update canonical `intermediate.mapped_responses` contract for current full mapping scope.
- **PR B:** refactor `mapped_responses` to compose foreign-ownership + cell-number seams and write canonical `intermediate.mapped_responses`.
- **PR C:** move/compose cell-number mapped-output checks onto canonical `intermediate.mapped_responses` with production-friendly check names/messages.
- **PR D:** update chain smoke test assertions for canonical mapped output.
- **PR E:** deprecate or mark `intermediate.cell_number_mapped_responses` as debug/checkpoint-only (without immediate deletion).
- **PR F:** only after stabilization, proceed to next mapping seam inventory/implementation.

## Risks and mitigations

1. **Risk:** breaking current tests during semantic migration.  
   **Mitigation:** small PRs, progressive test updates, and no checkpoint test removal until canonical replacements pass.

2. **Risk:** losing seam-level debugging visibility.  
   **Mitigation:** retain temporary checkpoint outputs where justified and keep seam-level domain tests.

3. **Risk:** `mapped_responses` implementation becoming too large/opaque.  
   **Mitigation:** strict domain-function composition with explicit seam boundaries.

4. **Risk:** confusion in table semantics during transition.  
   **Mitigation:** explicit documentation, phased rollout, and clear production-vs-checkpoint check labeling.

## Anti-goals

This plan does **not**:
- implement consolidation;
- remove current assets/tables;
- add new mapping seams;
- start imputation;
- reintroduce or lift-and-shift legacy `run_mapping`.
