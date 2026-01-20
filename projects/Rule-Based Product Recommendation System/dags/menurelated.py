from pathlib import Path
import sys

# cari parent yang punya folder 'scripts' (naik sampai root)
p = Path.cwd().resolve()
for parent in [p] + list(p.parents):
    if (parent / "scripts").is_dir():
        project_root = parent
        break
else:
    raise RuntimeError("Could not find 'scripts' folder in parent path")

sys.path.insert(0, str(project_root))
print("Added to sys.path:", project_root)

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
# from esb_functions import sendMessageToGoogleChat
from scripts.functions import airflow_task_failure_alert
import pendulum

# from airflow.decorators import task

default_args = {
    "owner": "Budi Kukuh",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": airflow_task_failure_alert,
}

with DAG(
    dag_id="menurelated",
    description="Rule-Based Product Recommendation System",
    tags=[
        "weekly",
    ],
    default_args=default_args,
    start_date=pendulum.datetime(2023, 2, 9, tz="Asia/Jakarta"),
    schedule_interval="25 3 * * SUN",
    catchup=False,
) as dag:
    salesMenu = BashOperator(
        task_id="Upsert Sales Menu",
        bash_command="""
        echo data_interval_start = {{ data_interval_start }} && 
        echo data_interval_end = {{ data_interval_end }} && 
        python3.9 /etl/salesmenu.py
        """,
    )

    salesMenuEzo = BashOperator(
        task_id="Upsert Sales Menu EZO",
        bash_command="""
        echo data_interval_start = {{ data_interval_start }} && 
        echo data_interval_end = {{ data_interval_end }} && 
        python3.9 /etl/salesMenuEzo.py
        """,
    )

    menuPairing = BashOperator(
        task_id="Upsert Top Menu Related",
        bash_command="""
        echo data_interval_start = {{ data_interval_start }} && 
        echo data_interval_end = {{ data_interval_end }} && 
        python3.9 /etl/menuPairing.py
        """,
    )

    salesMenu >> salesMenuEzo >> menuPairing
