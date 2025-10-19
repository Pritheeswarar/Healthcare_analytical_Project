# Staging Strategy Decision

## Option 1: On-demand Views (Current)

- **Pros:** No storage duplication, always reflects latest source data, minimal load pipeline.
- **Cons:** Expensive string/date normalization performed on every read; non-sargable expressions prevent index usage; severe latency observed (minutes for tens of rows). No opportunity to index intermediate results.

## Option 2: Materialized Staging Tables

- **Approach:** Nightly batch populates tables `stg.patients_std_tbl`, etc. Steps: truncate, load from source (or switch partitions), apply cleansing once via set-based INSERT…SELECT using persisted helper tables (Numbers, date dimension). Add targeted nonclustered indexes.
- **Pros:** Cleansing cost paid once per load; queries become fast lookups; indexes enforce data types and provide seek predicates. Allows persisted computed columns for regularly used expressions.
- **Cons:** Requires ETL job maintenance; data freshness tied to batch frequency; additional storage.
- **Recommendation:** Preferred path. Estimated to cut query latency from minutes to milliseconds once typed columns indexed.

## Option 3: Indexed Views

- **Approach:** Convert staging views to `WITH SCHEMABINDING`, create clustered index. Materializes data automatically.
- **Pros:** Simplifies pipeline relative to separate tables; SQL Server maintains index on change.
- **Cons:** Requires deterministic expressions and schema binding (current CROSS APPLY pattern must be rewritten). Indexed views add overhead to source DML and may not be supported if underlying tables are bulk-loaded.

## Migration Sketch

1. Create helper table `dbo.NumberTable` (1..500) and persist.
2. Build new staging tables with typed columns, load via stored procedure using existing cleansing logic (rewritten to be set-based).
3. Add indexes from `index_recommendations.sql`.
4. Swap consumers to use tables; keep old views for compatibility or redefine views to select from tables.
5. Consider incremental loads (partition switch) if source is large.

Given the current latency, moving to materialized staging tables (Option 2) provides the highest impact with manageable code churn. Indexed views could be explored later once expressions are simplified.
