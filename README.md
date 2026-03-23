# Data Validation Framework

## Technologies
Python | SQL | PostgreSQL | Apache Airflow | AWS RDS | AWS S3

## Description
Automated framework to validate accuracy and consistency of data 
between source and target systems in ETL pipelines. Performs 
configurable data quality checks and stores results for auditing.

## Architecture

Source DB (RDS) → Python Engine → Target DB (RDS)
                       ↓
              S3 (validation_rules.json)
                       ↓
              Audit Table (PostgreSQL)
                       ↓
              Apache Airflow DAG (Orchestration)
```

## Validation Checks
| Check | Result | Details |
|---|---|---|
| Row count | FAIL | Source:10 Target:8 — 2 rows missing |
| Duplicate detection | PASS | No duplicates found |
| Null validation | PASS | No nulls in customer_id |
| Aggregate sum | FAIL | Source:2255.9 Target:2099.92 |

## Project Structure
```
├── validator.py          # Python validation engine
├── config.py             # DB and S3 configuration
├── validation_dag.py     # Apache Airflow DAG
└── validation_rules.json # Metadata-driven rules config
```

## How to Run
1. Configure AWS RDS endpoints in config.py
2. Upload validation_rules.json to S3
3. Run validator.py directly: `python validator.py`
4. Or trigger via Airflow DAG: `data_validation_pipeline`

## Audit Results Sample
```sql
SELECT * FROM validation_audit ORDER BY run_timestamp DESC;
```
```

Step 4 — Commit message:
```
Updated README with project details
```

Step 5 — Click `Commit changes`

---

## ✅ Your GitHub Link to Share:
```
https://github.com/Sujju555/data-validation-framework
