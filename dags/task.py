import pandas as pd
from functions import upload_arquivo
from datetime import datetime
from airflow.decorators import task


@task
def consulta_api_lake_bronze():
    from api_consulta_https import api_people, api_planets, api_films


    data_atual = datetime.now().strftime('%Y-%m-%d')

    pessoa = api_people(1)
    df_pessoa = pd.DataFrame([pessoa])

    planetas = api_planets(1)
    df_planetas = pd.DataFrame([planetas])

    filme = api_films(1)
    df_filme = pd.DataFrame([filme])

    upload_arquivo(data_atual, df_pessoa, df_planetas, df_filme, 'bronze')

@task
def bronze_silver_etl():
    from functions import padronizando_datas
    
    data_atual = datetime.now().strftime('%Y-%m-%d')

    pessoa_path = f'C:/projeto_final_dataops/dags/lake/bronze/people/pessoas_{data_atual}.csv'
    planetas_path = f'C:/projeto_final_dataops/dags/lake/bronze/planets/planetas_{data_atual}.csv'
    filme_path = f'C:/projeto_final_dataops/dags/lake/bronze/films/filmes_{data_atual}.csv'

    df_pessoa = pd.read_csv(pessoa_path)
    df_planetas = pd.read_csv(planetas_path)
    df_filme = pd.read_csv(filme_path)
    
    #ETL DATAFRAMES

    # padronizando datas dos datafranes
    dfs= [df_pessoa,df_planetas,df_filme]
    for df in dfs:
        padronizando_datas(df)

    upload_arquivo(data_atual, df_pessoa, df_planetas, df_filme, 'silver')


consulta_api_lake_bronze()
bronze_silver_etl()



