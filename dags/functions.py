from datetime import datetime
import os
import pandas as pd
import logging
import re

def aplicar_alteracoes(df, identificador):
    try:
        mapeamento_colunas = renomear_colunas()
        if identificador == 'filme':
            df = df.rename(columns=mapeamento_colunas[0]) # nomeando as colunas para Português
            df = df.applymap(lambda x: '' if x == '[]' else x) # alterando os valores [] para vazio
            df = remover_duplicados(df, 'numero_do_episodio')  #removendo duplicados
            df = calcular_porcentagem_nulos(df) # calculando porcentagem de nulos

        
        elif identificador == 'pessoa':
            df = df.rename(columns=mapeamento_colunas[1])  # nomeando as colunas para Português
            df = df.applymap(lambda x: '' if x == '[]' else x) # alterando os valores [] para vazio
            df = remover_duplicados(df, 'nome') #removendo duplicados
            df = calcular_porcentagem_nulos(df) # calculando porcentagem de nulos

            
        elif identificador == 'planetas':
            df = df.rename(columns=mapeamento_colunas[2])  # nomeando as colunas para Português
            df = remover_duplicados(df, 'nome') #removendo duplicados
            df = calcular_porcentagem_nulos(df) # calculando porcentagem de nulos
        else:
            raise ValueError("Identificador inválido. Deve ser 'filme', 'pessoa' ou 'planetas'.")
    except Exception as e:
        logging.info(f"Ocorreu um erro em 'aplicar_alteracoes': {str(e)}")
        return None 
 
    return df


def calcular_porcentagem_nulos(df):
    try:
        null_percentages = (df.isnull().sum() / len(df)) * 100

        result_df = pd.DataFrame(null_percentages).T
        result_df.columns = ['%' + col for col in result_df.columns]

        df = pd.concat([df, result_df], axis=1)
    except Exception as e:
        logging.info(f"Ocorreu um erro em 'calcular_porcentagem_nulos': {str(e)}")
        return None  
    return df

def remover_caracteres_especiais(df):
    try:
        for coluna in df.columns:
            df[coluna] = df[coluna].apply(lambda x: re.sub(r'[^\w\s]', '', x))
    except Exception as e:
        logging.info(f"Ocorreu um erro em 'remover_caracteres_especiais': {str(e)}")
        return None  
    return df

def remover_duplicados(df,colunas):
    try:
        df_sem_duplicatas = df.drop_duplicates(subset=[colunas])
    except Exception as e:
        logging.info(f"Ocorreu um erro em 'remover_duplicados': {str(e)}")
        return None
    return df_sem_duplicatas

def combinando_arquivos(diretorio):
   
    try:
        dfs = []
        for filename in os.listdir(diretorio):
            if filename.endswith('.csv'):
                filepath = os.path.join(diretorio, filename)
                df = pd.read_csv(filepath, sep=";")
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True)
    except Exception as e:
        logging.info(f"Ocorreu um erro em 'combinando_arquivos': {str(e)}")
        return None 
    
def remover_quebras_de_linha(df, caracteres_a_remover):
    try:
        for coluna in df.columns:
            if df[coluna].dtype == 'object':
                df[coluna] = df[coluna].str.replace(caracteres_a_remover, '')
    except Exception as e:
            logging.info(f"Ocorreu um erro em 'remover_quebras_de_linha': {str(e)}")
            return None
    return df


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
    try:
        pessoa_folder_path = os.path.join(base_path, path, 'people')
        planetas_folder_path = os.path.join(base_path, path, 'planets')
        filme_folder_path = os.path.join(base_path, path, 'films')

        for folder_path in [pessoa_folder_path, planetas_folder_path, filme_folder_path]:
            os.makedirs(folder_path, exist_ok=True)

        pessoa_csv_path = os.path.join(pessoa_folder_path, f'pessoas_{data_atual}.csv')
        planetas_csv_path = os.path.join(planetas_folder_path, f'planetas_{data_atual}.csv')
        filme_csv_path = os.path.join(filme_folder_path, f'filmes_{data_atual}.csv')

        df_pessoa.to_csv(pessoa_csv_path, index=False, sep=';')
        df_planetas.to_csv(planetas_csv_path, index=False, sep=';')
        df_filme.to_csv(filme_csv_path, index=False, sep=';')
        logging.info("Arquivos salvos com sucesso.")

    except OSError as e:
        logging.error(f"Erro ao criar diretórios: {str(e)}")
    except Exception as e:
        logging.error(f"Erro inesperado: {str(e)}")


def renomear_colunas():
    filme = {
        "title": "titulo",
        "episode_id": "numero_do_episodio",
        "opening_crawl": "introducao",
        "director": "diretor",
        "producer": "produtor",
        "release_date": "data_de_lancamento",
        "characters": "personagens",
        "planets": "planetas",
        "starships": "naves_estelares",
        "vehicles": "veiculos",
        "species": "especies",
        "created": "criado",
        "edited": "editado",
        "url": "URL",
        "Data Consulta": "Data Consulta"
    }
    pessoa = {
        "name": "nome",
        "height": "altura",
        "mass": "massa",
        "hair_color": "cor_do_cabelo",
        "skin_color": "cor_da_pele",
        "eye_color": "cor_dos_olhos",
        "birth_year": "ano_de_nascimento",
        "gender": "genero",
        "homeworld": "planeta_natal",
        "films": "filmes",
        "species": "especies",
        "vehicles": "veiculos",
        "starships": "naves_estelares",
        "created": "criado",
        "edited": "editado",
        "url": "URL",
        "Data Consulta": "Data Consulta"
    }
    planetas = {
        "name": "nome",
        "rotation_period": "periodo_de_rotacao",
        "orbital_period": "periodo_orbital",
        "diameter": "diametro",
        "climate": "clima",
        "gravity": "gravidade",
        "terrain": "terreno",
        "surface_water": "agua_superficial",
        "population": "populacao",
        "residents": "residentes",
        "films": "filmes",
        "created": "criado",
        "edited": "editado",
        "url": "URL",
        "Data Consulta": "Data Consulta"
    }
    return filme, pessoa, planetas
