The optimized staging layer exposes thin, type-enforcing views on top of raw landing tables so the transform layer can own business harmonization. Keep these objects focused on predictable schema cleanup, deterministic casting, and light standardization; escalate anything semantic or cross-entity to downstream phases.

- [ ] Do keep views set-based, deterministic, and sourced from `stg_optimized`.
- [ ] Don't introduce row-by-row UDFs or cursor-style processing.
- [ ] Don't add CROSS APPLY tokenization or parsing chains here; push to transform.
- [ ] Don't reintroduce RBAR logic; refactor to set-based patterns first.
