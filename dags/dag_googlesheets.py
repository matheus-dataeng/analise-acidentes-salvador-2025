import pendulum 
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.append("/opt/airflow")

from pipelines.pipeline_googlesheets import extract, load

with DAG(
    dag_id = "Analise_Acidentes_Salvador",
    description = "Analise de Acidentes em Salvador no ano de 2025",
    schedule = None,
    start_date = pendulum.datetime(2026, 1, 29, tz= "America/Sao_Paulo"),
    catchup = False,
    tags = ["Projeto", "Acidentes", "Salvador", "2025"]
)as dag:
    
    task_extract = PythonOperator(
        task_id = "Extract",
        python_callable = extract
    )
    
    task_load = PythonOperator(
        task_id = "Load",
        python_callable = load
    )

    task_extract >> task_load

    