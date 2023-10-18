from datetime import datetime
import os
import pandas as pd
import logging


def aplicar_alteracoes(df, identificador):
    mapeamento_colunas = renomear_colunas()

    if identificador == 'filme':
        df = df.rename(columns=mapeamento_colunas[0]) # nomeando as colunas para Português
        df = df.applymap(lambda x: '' if x == [] else x) # alterando os valores [] para vazio


       
    elif identificador == 'pessoa':
        df = df.rename(columns=mapeamento_colunas[1])  # nomeando as colunas para Português
        df = df.applymap(lambda x: '' if x == [] else x) # alterando os valores [] para vazio
        
    elif identificador == 'planetas':
        df = df.rename(columns=mapeamento_colunas[2])  # nomeando as colunas para Português
 

    return df

def remover_duplicados(dataframe,colunas):
  
    dataframe_sem_duplicatas = dataframe.drop_duplicates(subset=[colunas])
    return dataframe_sem_duplicatas

def combinando_arquivos(diretorio):
    dataframes = []
    for filename in os.listdir(diretorio):
        if filename.endswith('.csv'):
            filepath = os.path.join(diretorio, filename)
            df = pd.read_csv(filepath, sep=";")
            dataframes.append(df)
    return pd.concat(dataframes, ignore_index=True)

def remover_quebras_de_linha(df, caracteres_a_remover):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].str.replace(caracteres_a_remover, '')

def padronizar_data(data_original):
    try:
        data_hora_objeto = datetime.strptime(data_original, '%Y-%m-%dT%H:%M:%S.%fZ')
        data_formatada = data_hora_objeto.strftime('%d/%m/%Y')
        return data_formatada
    except ValueError:
        return data_original 

def padronizando_datas(df):
    for coluna in df.columns:
        if df[coluna].dtype == 'object':
            df[coluna] = df[coluna].apply(padronizar_data)

def upload_arquivo(data_atual, df_pessoa, df_planetas, df_filme, path):
    '''
    Originalmente, os arquivos estavam sendo salvos no interior do contêiner do 
    Airflow, e eu estava enfrentando dificuldades para visualizar as pastas e 
    acessar os dados. Para resolver esse problema, realizei uma configuração 
    no arquivo docker-compose utilizando o comando C:/projeto_final_dataops/dags/lake/:/opt/airflow/lake. 
    Essa configuração permite que o Docker Compose mapeie o 
    caminho da pasta C:/projeto_final_dataops/dags/lake/ do meu sistema Windows 
    para o caminho /opt/airflow/lake dentro do contêiner.

    Isso tem o efeito de permitir que o contêiner leia o caminho e os ]
    arquivos salvos no seu interior e, ao mesmo tempo, salve esses arquivos no 
    sistema Windows. Dessa forma, a base do caminho /opt/airflow/lake se 
    tornou um ponto de integração entre o contêiner do Airflow e o meu 
    sistema local, facilitando o gerenciamento e a visualização dos dados.
    '''

    base_path = '/opt/airflow/lake'

    pessoa_folder_path = os.path.join(base_path, path, 'people')
    planetas_folder_path = os.path.join(base_path, path, 'planets')
    filme_folder_path = os.path.join(base_path, path, 'films')

    for folder_path in [pessoa_folder_path, planetas_folder_path, filme_folder_path]:
        os.makedirs(folder_path, exist_ok=True)

    pessoa_csv_path = os.path.join(pessoa_folder_path, f'pessoas_{data_atual}_1.csv')
    planetas_csv_path = os.path.join(planetas_folder_path, f'planetas_{data_atual}_1.csv')
    filme_csv_path = os.path.join(filme_folder_path, f'filmes_{data_atual}_1.csv')

    logging.info(f'Caminho de pessoas: {pessoa_csv_path}')
    logging.info(f'Caminho de planetas: {planetas_csv_path}')
    logging.info(f'Caminho de filmes: {filme_csv_path}')

    df_pessoa.to_csv(pessoa_csv_path, index=False, sep=';')
    df_planetas.to_csv(planetas_csv_path, index=False, sep=';')
    df_filme.to_csv(filme_csv_path, index=False, sep=';')
    logging.info("Arquivos salvos com sucesso.")



def renomear_colunas():
    filme = {
        "title": "título",
        "episode_id": "número_do_episódio",
        "opening_crawl": "introdução",
        "director": "diretor",
        "producer": "produtor",
        "release_date": "data_de_lançamento",
        "characters": "personagens",
        "planets": "planetas",
        "starships": "naves_estelares",
        "vehicles": "veículos",
        "species": "espécies",
        "created": "criado",
        "edited": "editado",
        "url": "URL"
    }
    pessoa = {
        "name": "nome",
        "height": "altura",
        "mass": "massa",
        "hair_color": "cor_do_cabelo",
        "skin_color": "cor_da_pele",
        "eye_color": "cor_dos_olhos",
        "birth_year": "ano_de_nascimento",
        "gender": "gênero",
        "homeworld": "planeta_natal",
        "films": "filmes",
        "species": "espécies",
        "vehicles": "veículos",
        "starships": "naves_estelares",
        "created": "criado",
        "edited": "editado",
        "url": "URL"
    }
    planetas = {
        "name": "nome",
        "rotation_period": "período_de_rotação",
        "orbital_period": "período_orbital",
        "diameter": "diâmetro",
        "climate": "clima",
        "gravity": "gravidade",
        "terrain": "terreno",
        "surface_water": "água_superficial",
        "population": "população",
        "residents": "residentes",
        "films": "filmes",
        "created": "criado",
        "edited": "editado",
        "url": "URL"
    }
    return filme, pessoa, planetas