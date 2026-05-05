from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import vertica_python

conn_info = {
    'host': 'vertica.data-engineer.education-services.ru',
    'port': '5433',
    'user': 'VT260427EBD88C',
    'password': '5b461a9e494549c5b070f9c8cdaf2ec9',
    'database': 'dwh',
    'autocommit': True
}

def load_group_log():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute("""
            COPY VT260427EBD88C__STAGING.group_log (
                group_id, user_id, user_id_from, event, datetime
            )
            FROM '/data/group_log.csv'
            DELIMITER ','
            ENCLOSED BY '"'
            NULL AS ''
            SKIP 1
        """)
        cur.execute("SELECT COUNT(*) FROM VT260427EBD88C__STAGING.group_log")
        count = cur.fetchone()[0]
        print(f"Loaded {count} rows into group_log")

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
}

with DAG(
    'load_group_log',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    
    load_task = PythonOperator(
        task_id='load_group_log',
        python_callable=load_group_log,
    )