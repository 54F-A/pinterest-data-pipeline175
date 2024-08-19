from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta 


notebook_task = {
    'notebook_path': '/Users/sarfaraj_ahmed@outlook.com/Clean',
}


notebook_params = {
    "Variable":5
}


default_args = {
    'owner': '12aa97d84d77',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


with DAG(
    '12aa97d84d77_dag',
    start_date=datetime(2024, 8, 6, 0, 0, 0),
    schedule_interval='0 0 * * *',
    catchup=False,
    default_args=default_args
) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run