from airflow import DAG
from datetime import datetime
from task import consulta_api_lake_bronze,bronze_silver_etl

with DAG(
    "projeto_final",
    start_date        = datetime(2021, 1 ,1),
    schedule_interval = '@daily',
    catchup = False               
) as dag:
    t1 = consulta_api_lake_bronze()
    t2 = bronze_silver_etl()
    
    t1 >> t2