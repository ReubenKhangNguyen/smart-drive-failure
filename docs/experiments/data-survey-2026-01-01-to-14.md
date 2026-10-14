# Data survey: 2026-01-01 to 2026-01-14

## Purpose

This survey checks whether the Silver Backblaze data is usable for later feature engineering. It is descriptive Spark SQL analysis only; it does not train or evaluate a prediction model.

## Input and processing

- Input: `hdfs://namenode:9000/smart-drive/silver/daily`
- Job: `analytics/survey_gold.py`
- Output: `hdfs://namenode:9000/smart-drive/gold/analytics`
- Runtime: 111.31 seconds on one Spark Master and two Spark Workers.

## Dataset overview

| Measure | Result |
|---|---:|
| Observation dates | 14 |
| Drive-day observations | 4,731,145 |
| Distinct drives | 339,692 |
| Drive models | 78 |
| Datacenters | 6 |
| Recorded failure rows | 109 |
| Date range | 2026-01-01 to 2026-01-14 |

## Quality checks after Silver ETL

| Check | Result |
|---|---:|
| Null date rows | 0 |
| Null serial-number rows | 0 |
| Null model rows | 0 |
| Invalid `failure` rows | 0 |
| Duplicate `(date, serial_number)` rows | 0 |

## Initial descriptive findings

- Recorded failures are rare: 109 of 4,731,145 observations (about 0.0023%). This indicates an imbalanced classification problem for the later ML phase.
- The largest daily failure count in this subset is 16 on 2026-01-06; the next is 14 on 2026-01-13.
- `ST12000NM0008` has 21 recorded failure rows in the subset. Counts and rates must be read with the number of observations for each model; they are not evidence of a causal model effect.
- The average values of `smart_5_raw`, `smart_187_raw`, `smart_197_raw`, and `smart_198_raw` are much higher in rows with a recorded failure than in non-failure rows. This is a screening result only; future feature engineering must avoid using the current-day `failure` as a feature and must validate any predictive association using a forward-looking label.

## Evidence artifacts

- Local JSON copies: `artifacts/reports/data_survey/`
- Spark job log: `artifacts/reports/data-survey.log`
- Distributed Parquet tables: `/smart-drive/gold/analytics`
- `hdfs fsck /smart-drive/gold/analytics` reports five data blocks with average replication 2.0 and status `HEALTHY`.
