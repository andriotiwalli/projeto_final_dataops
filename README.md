# projeto_final_dataops

## Estrutura do Projeto
dags/projeto_final.py: O arquivo principal que define o DAG do Apache Airflow.
functions.py: Contém funções auxiliares usadas no processo de ETL.
api_consulta_https.py: Contém funções para fazer consultas à API Star Wars.
task.py: Define as tarefas que compõem o DAG do Airflow.
lake/bronze, lake/silver, lake/gold: Pastas usadas para armazenar os diferentes estágios dos dados.

## DAG - projeto_final
O DAG projeto_final define as tarefas do pipeline e sua ordem de execução. O pipeline consiste em três tarefas principais:

consulta_api_lake_bronze: Esta tarefa consulta a API Star Wars para obter informações aleatórias de pessoas, planetas e filmes. Os dados são armazenados na camada "bronze" do data lake.

bronze_silver_etl: Realiza as transformações nos dados da camada "bronze" para prepará-los para a camada "silver" do data lake. Isso inclui padronização de datas, remoção de quebras de linha e caracteres especiais, e cálculo da porcentagem de valores nulos.

silver_to_gold: Combina os dados da camada "silver" para criar um único conjunto de dados, aplicando transformações adicionais específicas para cada tipo de dado (filme, pessoa, planetas). Os dados transformados são armazenados na camada "gold" do data lake.

## Funções Auxiliares
aplicar_alteracoes: Realiza transformações específicas para cada tipo de dado (filme, pessoa, planetas), como renomear colunas, lidar com valores nulos e remover duplicatas.

combinando_arquivos: Combina arquivos de dados da camada "silver" em um único DataFrame.

remover_quebras_de_linha: Remove quebras de linha de colunas de texto em um DataFrame.

padronizar_data e padronizando_datas: Padronizam datas em um formato específico.

upload_arquivo: Salva DataFrames em arquivos CSV na estrutura de diretórios apropriada na camada "bronze," "silver" ou "gold."

## Executando o DAG
O DAG é agendado para ser executado diariamente às 8h (schedule_interval='0 8 * * *'). Certifique-se de que o Apache Airflow esteja configurado e em execução para agendar e executar as tarefas.
