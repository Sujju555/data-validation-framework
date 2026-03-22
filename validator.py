import psycopg2
import boto3
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from config import SOURCE_DB, TARGET_DB, S3_BUCKET, S3_KEY

def get_connection(db_config):
    return psycopg2.connect(**db_config)

def load_rules_from_s3():
    s3 = boto3.client("s3", region_name="us-east-1")
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    rules = json.loads(obj["Body"].read().decode("utf-8"))
    logger.info(f"Loaded {len(rules)} validation rules from S3")
    return rules

def log_to_audit(check_name, table_name, status, 
                 source_value, target_value, difference):
    conn = get_connection(TARGET_DB)
    cur  = conn.cursor()
    cur.execute("""
        INSERT INTO validation_audit 
        (check_name, table_name, status, 
         source_value, target_value, difference)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (check_name, table_name, status,
          source_value, target_value, difference))
    conn.commit()
    cur.close()
    conn.close()

def check_row_count(rule):
    src_conn = get_connection(SOURCE_DB)
    tgt_conn = get_connection(TARGET_DB)
    src_cur  = src_conn.cursor()
    tgt_cur  = tgt_conn.cursor()

    src_cur.execute(f"SELECT COUNT(*) FROM {rule['source_table']}")
    tgt_cur.execute(f"SELECT COUNT(*) FROM {rule['target_table']}")

    src_count = src_cur.fetchone()[0]
    tgt_count = tgt_cur.fetchone()[0]
    diff      = src_count - tgt_count
    status    = "PASS" if diff == 0 else "FAIL"

    logger.info(f"Row count → Source:{src_count} Target:{tgt_count} → {status}")
    log_to_audit("row_count", rule["source_table"], status,
                 src_count, tgt_count, diff)

    src_cur.close(); tgt_cur.close()
    src_conn.close(); tgt_conn.close()

def check_duplicates(rule):
    conn = get_connection(TARGET_DB)
    cur  = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT {rule['key']}, COUNT(*)
            FROM {rule['table']}
            GROUP BY {rule['key']}
            HAVING COUNT(*) > 1
        ) dups
    """)
    dup_count = cur.fetchone()[0]
    status    = "PASS" if dup_count == 0 else "FAIL"

    logger.info(f"Duplicate check → {dup_count} duplicates → {status}")
    log_to_audit("duplicate", rule["table"], status, 0, dup_count, dup_count)

    cur.close(); conn.close()

def check_nulls(rule):
    conn = get_connection(TARGET_DB)
    cur  = conn.cursor()
    cur.execute(f"""
        SELECT COUNT(*) FROM {rule['table']}
        WHERE {rule['column']} IS NULL
    """)
    null_count = cur.fetchone()[0]
    status     = "PASS" if null_count == 0 else "FAIL"

    logger.info(f"Null check → {null_count} nulls in {rule['column']} → {status}")
    log_to_audit("null_check", rule["table"], status, 0, null_count, null_count)

    cur.close(); conn.close()

def check_aggregate(rule):
    src_conn = get_connection(SOURCE_DB)
    tgt_conn = get_connection(TARGET_DB)
    src_cur  = src_conn.cursor()
    tgt_cur  = tgt_conn.cursor()

    src_cur.execute(f"SELECT {rule['type']}({rule['column']}) FROM {rule['table']}")
    tgt_cur.execute(f"SELECT {rule['type']}({rule['column']}) FROM {rule['table']}")

    src_val = float(src_cur.fetchone()[0])
    tgt_val = float(tgt_cur.fetchone()[0])
    diff    = src_val - tgt_val
    status  = "PASS" if diff == 0 else "FAIL"

    logger.info(f"Aggregate {rule['type']}({rule['column']}) → Source:{src_val} Target:{tgt_val} → {status}")
    log_to_audit(f"aggregate_{rule['type']}", rule["table"], status,
                 src_val, tgt_val, diff)

    src_cur.close(); tgt_cur.close()
    src_conn.close(); tgt_conn.close()

def run_validations():
    logger.info("=== Starting Data Validation Framework ===")
    rules = load_rules_from_s3()

    for rule in rules:
        if rule["check"] == "row_count":
            check_row_count(rule)
        elif rule["check"] == "duplicate":
            check_duplicates(rule)
        elif rule["check"] == "null_check":
            check_nulls(rule)
        elif rule["check"] == "aggregate":
            check_aggregate(rule)

    logger.info("=== Validation Complete ===")

if __name__ == "__main__":
    run_validations()