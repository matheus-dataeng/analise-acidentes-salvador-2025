import pendulum 
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys 

sys.path.append("/opt/airflow")

from pipelines.pipeline_geral import extract, transform, load

with DAG(
    dag_id = "Acidentes_Salvador",
    description = "Acidentes Salvador 2025, periodos festivos",
    start_date = pendulum.datetime(2026, 1 ,10, tz= "America/Sao_Paulo"),
    schedule = None,
    catchup = False,
    tags = ["Projeto", "Acidentes", "DataOps"]
) as dag:

    def run_pipeline():
        dados_brutos = extract()
        dataframes = transform(dados_brutos)
        load(dataframes)


    task_pipeline = PythonOperator(
        task_id = "pipeline",
        python_callable = run_pipeline
    )
    