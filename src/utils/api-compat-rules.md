# API Compatibility Rules

Phase 5.x changes must keep mobile and backend clients backwards compatible.

- New API fields are additive only. Do not delete existing fields from public responses.
- Grade enum values remain `GO`, `CAUTION`, and `AVOID`. WAIT is derived from wait metadata, not a new persisted grade value.
- Narrative shape remains stable: existing `narrative.why_now`, `risk_note`, `key_events`, `source_quality`, and `engine_version` fields are preserved.
- `composite_score` remains present even if a later phase introduces `explanatory_score`; expose the new name additively.
- Mobile builds must tolerate missing additive fields and render a neutral fallback instead of crashing.
- Schema migrations must be idempotent when possible and safe to run more than once.
