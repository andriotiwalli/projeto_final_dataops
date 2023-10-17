from datetime import datetime
import os


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

    pessoa_csv_path = os.path.join(pessoa_folder_path, f'pessoas_{data_atual}.csv')
    planetas_csv_path = os.path.join(planetas_folder_path, f'planetas_{data_atual}.csv')
    filme_csv_path = os.path.join(filme_folder_path, f'filmes_{data_atual}.csv')

    print(f'Caminho de pessoas: {pessoa_csv_path}')
    print(f'Caminho de planetas: {planetas_csv_path}')
    print(f'Caminho de filmes: {filme_csv_path}')

    df_pessoa.to_csv(pessoa_csv_path, index=False)
    df_planetas.to_csv(planetas_csv_path, index=False)
    df_filme.to_csv(filme_csv_path, index=False)
    print("Arquivos salvos com sucesso.")
