from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from flight_tracker_utils import fetch_states, load_states

def poll_and_load():
    df = fetch_states()
    load_states(df)

with DAG(
    dag_id="flight_tracker_poll",
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(seconds=30),   # this replaces your while-loop timing
    catchup=False,
    max_active_runs=1,                # don't let runs pile up if one is slow
) as dag:

    poll_task = PythonOperator(
        task_id="poll_and_load",
        python_callable=poll_and_load,
    )