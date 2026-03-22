from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.insert(0, '/home/sujaydesai/airflow/dags')

from validator import check_row_count, check_duplicates, check_nulls, check_aggregate

default_args = {
    'owner'           : 'sujay',
    'retries'         : 1,
    'retry_delay'     : timedelta(minutes=2)
}

with DAG(
    dag_id          = 'data_validation_pipeline',
    default_args    = default_args,
    description     = 'Automated Data Validation Framework',
    schedule        = '@daily',
    start_date      = datetime(2024, 1, 1),
    catchup         = False,
    tags            = ['validation', 'data-quality']
) as dag:

    t1 = PythonOperator(
        task_id         = 'check_row_count',
        python_callable = check_row_count,
        op_args         = [{"check": "row_count", "source_table": "orders", "target_table": "orders"}]
    )

    t2 = PythonOperator(
        task_id         = 'check_duplicates',
        python_callable = check_duplicates,
        op_args         = [{"check": "duplicate", "table": "orders", "key": "order_id"}]
    )

    t3 = PythonOperator(
        task_id         = 'check_nulls',
        python_callable = check_nulls,
        op_args         = [{"check": "null_check", "table": "orders", "column": "customer_id"}]
    )

    t4 = PythonOperator(
        task_id         = 'check_aggregate',
        python_callable = check_aggregate,
        op_args         = [{"check": "aggregate", "table": "orders", "column": "amount", "type": "sum"}]
    )

    t1 >> t2 >> t3 >> t4
