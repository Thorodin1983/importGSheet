import pyodbc
import pandas as pd
from google.cloud import bigquery

# Defina o caminho para o arquivo de chave JSON
key_path = 'C:\\Users\\thorodin\\Desktop\\api_google\\ethereal-terra-429616-s2-48bf204a3f24.json'

client = bigquery.Client.from_service_account_json(key_path)

conn_str = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=SERVDB4\\SQLEXPRESS;'
    'DATABASE=futebol;'
    'UID=sa;'
    'PWD=Luan@1983;'
)
conn = pyodbc.connect(conn_str)

query = 'SELECT id_team, team, liga, country FROM clube'
df = pd.read_sql(query, conn)

conn.close()


dataset_id = 'futebol'  # Nome do dataset
table_id = 'clube'  # Nome da tabela
table_ref = client.dataset(dataset_id).table(table_id)

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("id_team", "INTEGER"),
        bigquery.SchemaField("team", "STRING"),
        bigquery.SchemaField("liga", "STRING"),
        bigquery.SchemaField("country", "STRING"),
    ],
    write_disposition='WRITE_TRUNCATE',  # Substituir dados existentes
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Ignorar a primeira linha do CSV que contém os cabeçalhos
)

# Salve o DataFrame em um arquivo CSV temporário
csv_path = 'C:\\Users\\thorodin\\Desktop\\upload_gsheet\\temp\\arquivo_temporario.csv'
df.to_csv(csv_path, index=False)

with open(csv_path, 'rb') as source_file:
    job = client.load_table_from_file(
        source_file,
        table_ref,
        job_config=job_config
    )

print("Iniciando o trabalho de carga de dados...")

job.result()

print(f'Tabela {table_id} carregada com sucesso!')
