from airflow import DAG
from datetime import datetime
from task import consulta_api_lake_bronze, bronze_silver_etl, silver_to_gold

with DAG(
    "projeto_final",
    start_date = datetime(2021, 1 ,1),
    schedule_interval='0 8 * * *',    
    catchup = False               
) as dag:
    t1 = consulta_api_lake_bronze()
    t2 = bronze_silver_etl()
    t3 = silver_to_gold()
    
    t1 >> t2 >> t3