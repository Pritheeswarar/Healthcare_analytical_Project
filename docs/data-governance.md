# Data Governance: HIPAA Safe Harbor

The HIPAA Privacy Rule describes a Safe Harbor method for de-identification. It requires removing 18 direct identifiers of the patient, relatives, employers, or household members and ensuring the remaining information cannot reasonably identify an individual. The guidance below adapts the rule for analytics use within this repository.

## Identifiers to Remove or Obscure

- Direct contact info: names, street addresses, telephone numbers, email addresses, fax numbers.
- Digital identifiers: Social Security numbers, medical record numbers, account numbers, certificate and license numbers, full-face photos.
- Device and network traces: IP and MAC addresses, URLs, biometric identifiers.
- Location granularity: geocodes smaller than state, precise facility room numbers.
- Temporal detail: all elements of dates (except year) related to an individual, including birth, admission, discharge, death, and appointment timestamps.

## Handling Admission and Discharge Dates

- Store full timestamps only in secured, governed staging tables (`sql/01_staging/`).
- For analytics exports, bucket admission and discharge dates to the first day of the month (e.g., `2025-10-01`).
- When day-level trends are required, apply a deterministic but undocumented day shift of +/- up to 3 days per encounter and document the shift logic in internal notes (never in public docs).
- Suppress any date values for patients aged 89 and older, replacing with a capped age band (e.g., `90+`).

## Patient Identifier Strategy

- Generate a salted SHA-256 hash of the source patient identifier (`patient_hash = SHA256(patient_id + secret_salt)`).
- Maintain the secret salt in a secure secrets manager; do not store it in git, configuration files, or notebooks.
- Rotate the salt annually and re-hash historical extracts when rotation occurs.
- Use surrogate encounter hashes when a single patient spans multiple visits to avoid linkage back to the source system.

## Export Allowance Matrix

| Status | Examples | Export Guidance |
|--------|----------|-----------------|
| Red    | Names, full addresses, MRNs, Social Security numbers, detailed admission/discharge timestamps, images | Never export outside the secure analytics environment. Remove before running KPI extracts. |
| Yellow | Month-level admission/discharge buckets, hashed patient IDs, ZIP3, service-line labels, provider specialty | Export only to controlled consumers after documenting purpose, data handling, and retention. Include caveats in PR notes. |
| Green  | Aggregated KPIs (counts, rates), calendar year fields, facility-level occupancy trends, payer mix percentages, CPT/ICD leaderboards without cell counts < 11 | Safe to export when calculations preserve aggregation thresholds and no combination enables re-identification. |

## Analyst Checklist

- Review queries for direct identifiers prior to moving SQL into `sql/03_kpi/`.
- Validate that date bucketing or shifting logic is applied consistently.
- Confirm hashing functions reference the current salt and are executed within the database (no local exports of raw IDs).
- Document any residual risk in the accompanying analysis ticket and highlight it during code review.
