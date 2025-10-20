# Deprecations

- `sql/01_staging/`: Retired heavy staging views in favor of `sql/01_staging_optimized/` thin, type-enforcing views. All business logic now flows through the transform layer.
