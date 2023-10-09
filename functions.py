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

def upload_arquivo(data_atual, df_pessoa, df_planetas, df_filme,path):
    pessoa_folder_path = f'C:/projeto_final_dataops/lake/{path}/people'
    planetas_folder_path = f'C:/projeto_final_dataops/lake/{path}/planets'
    filme_folder_path = f'C:/projeto_final_dataops/lake/{path}/films'

    pessoa_csv_path = os.path.join(pessoa_folder_path, f'pessoas_{data_atual}.csv')
    planetas_csv_path = os.path.join(planetas_folder_path, f'planetas_{data_atual}.csv')
    filme_csv_path = os.path.join(filme_folder_path, f'filmes_{data_atual}.csv')

    df_pessoa.to_csv(pessoa_csv_path, index=False)
    df_planetas.to_csv(planetas_csv_path, index=False)
    df_filme.to_csv(filme_csv_path, index=False)