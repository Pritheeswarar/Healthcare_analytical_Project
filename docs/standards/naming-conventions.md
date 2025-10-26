# T-SQL Naming Conventions

## Core Rules

- Use `snake_case` for all schema, table, column, CTE, and function identifiers.
- Prefix views with `v_` (e.g., `v_kpi_occupancy`) to distinguish from base tables.
- Avoid `sp_` prefixes; SQL Server reserves them for system stored procedures.
- Name primary key indexes `pk_<table_name>` and foreign key indexes `fk_<table_name>_<referenced_table>`.
- Name unique indexes `ux_<table_name>_<column_group>` and nonclustered indexes `ix_<table_name>_<column_group>`.
- Reserve schema names for domain areas (`dw`, `finance`, `clinical`) and avoid defaulting to `dbo` for new assets.

## Data Type Preferences

- Dates and times: use `datetime2(0-7)` depending on precision; default to `datetime2(3)` for audit columns.
- Monetary values: prefer `decimal(19,4)` to avoid rounding issues in the legacy `money` type.
- Boolean flags: default to `bit`. Use `tinyint` only when more than two states are required and document the mapping.
- Surrogate keys: use `bigint` identity columns when key counts may exceed 2^31.

## Column Naming Patterns

- Primary keys: `<entity>_id` (e.g., `encounter_id`).
- Foreign keys: `<referenced_entity>_id` (e.g., `patient_id`).
- Audit columns: `created_at`, `created_by`, `updated_at`, `updated_by`.
- Effective dating: `effective_start_date`, `effective_end_date`.
- Metrics: use measurement and unit (`los_days`, `lab_turnaround_minutes`).

## File Header Template

Add the following header block at the top of each SQL file to capture lineage and context:

```sql
/*
File: <path/to/file.sql>
Author: <name or team>
Description: <short purpose statement>
Dependencies: <upstream objects or views>
Change Log:
  - YYYY-MM-DD: <summary of change>
*/
```

## Inline Commenting

- Use single-line comments (`--`) to explain non-obvious logic, especially around business rules or deviation from standards.
- Place comments above the relevant statement; avoid trailing comments that wrap mid-line.
- Reference requirement IDs, tickets, or regulatory drivers when logic supports compliance reporting.
