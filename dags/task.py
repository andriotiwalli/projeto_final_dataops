import pandas as pd
from functions import upload_arquivo
from datetime import datetime
from airflow.decorators import task
import random


data_atual = datetime.now().strftime('%Y-%m-%d')


@task
def consulta_api_lake_bronze():
    from api_consulta_https import api_people, api_planets, api_films
  
    pessoa = api_people(random.randint(1, 15))
    df_pessoa = pd.DataFrame([pessoa])
     
    planetas = api_planets(random.randint(1, 5))
    df_planetas = pd.DataFrame([planetas])
   
    filme = api_films(random.randint(1, 7))
    df_filme = pd.DataFrame([filme])
 
    upload_arquivo(data_atual, df_pessoa, df_planetas, df_filme, 'bronze')

@task
def bronze_silver_etl():
    from functions import padronizando_datas,remover_quebras_de_linha, upload_arquivo
    
    
    #/opt/airflow/ > local que esta sendo salvo o arquivo no docker
    pessoa_path = f'/opt/airflow/lake/bronze/people/pessoas_{data_atual}_1.csv'
    planetas_path = f'/opt/airflow/lake/bronze/planets/planetas_{data_atual}_1.csv'
    filme_path = f'/opt/airflow/lake/bronze/films/filmes_{data_atual}_1.csv'

    df_pessoa = pd.read_csv(pessoa_path, sep=';')
    df_planetas = pd.read_csv(planetas_path, sep=';')
    df_filme = pd.read_csv(filme_path,  sep=';')
    
    #ETL DATAFRAMES
    caracteres_a_remover = r'[\r\n]'  

    dfs= [df_pessoa,df_planetas,df_filme]
    for df in dfs:
        padronizando_datas(df)
        remover_quebras_de_linha(df, caracteres_a_remover)
          

    upload_arquivo(data_atual, df_pessoa, df_planetas, df_filme, 'silver')



@task
def silver_to_gold():
    from functions import combinando_arquivos, upload_arquivo, remover_duplicados, aplicar_alteracoes

    df_pessoa = combinando_arquivos('/opt/airflow/lake/silver/people/')
    df_planetas = combinando_arquivos('/opt/airflow/lake/silver/planets/')
    df_filme = combinando_arquivos('/opt/airflow/lake/silver/films/')

    dataframes = [df_pessoa, df_planetas, df_filme]

    #removendo duplicados
    df_pessoa = remover_duplicados(df_pessoa, 'name')
    df_planetas = remover_duplicados(df_planetas, 'name')
    df_filme = remover_duplicados(df_filme, 'episode_id')

    
    df_filme = aplicar_alteracoes(df_filme, 'filme')
    df_pessoa = aplicar_alteracoes(df_pessoa, 'pessoa')
    df_planetas = aplicar_alteracoes(df_planetas, 'planetas')

    upload_arquivo('all', df_pessoa, df_planetas, df_filme, 'gold')
    
    return 
