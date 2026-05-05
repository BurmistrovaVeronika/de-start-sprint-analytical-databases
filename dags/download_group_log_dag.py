from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'me',
    'start_date': datetime(2022, 1, 1),
}

with DAG(
    'download_group_log',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    download = BashOperator(
        task_id='download_group_log',
        bash_command='mkdir -p /data && wget -O /data/group_log.csv https://storage.yandexcloud.net/sprint6/group_log.csv',
    )