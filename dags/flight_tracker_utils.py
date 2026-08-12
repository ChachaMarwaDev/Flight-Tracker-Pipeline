from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from flight_tracker_utils import ensure_table, fetch_states, load_states


def poll_and_load():
    df = fetch_states()
    load_states(df)


with DAG(
    dag_id="flight_tracker_poll",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=2),   # start conservative; tighten later once stable
    catchup=False,
    max_active_runs=1,               # don't let a slow run overlap with the next trigger
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=15),
    },
) as dag:

    create_table_task = PythonOperator(
        task_id="ensure_table",
        python_callable=ensure_table,
    )

    poll_task = PythonOperator(
        task_id="poll_and_load",
        python_callable=poll_and_load,
    )

    create_table_task >> poll_task