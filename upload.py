import pyodbc
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials

connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=<instancia>;' 
    'DATABASE=<database>;'
    'UID=<usr>;'
    'PWD=<senha>'
)

try:
    conn = pyodbc.connect(connection_string)
    print("Conexão estabelecida com sucesso!")
except Exception as e:
    print(f"Erro ao conectar: {e}")

query = "SELECT * FROM clube"

try:
    df = pd.read_sql(query, conn)
    print("Dados carregados com sucesso!")
    print(df.head()) 
except Exception as e:
    print(f"Erro ao carregar dados: {e}")

conn.close()

def convert_to_serializable(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)
        elif df[col].dtype == 'object':
            df[col] = df[col].astype(str)
    return df

df = convert_to_serializable(df)

scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
         "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

creds = ServiceAccountCredentials.from_json_keyfile_name('C:\\Users\\thorodin\\Desktop\\api_google\\ethereal-terra-429616-s2-48bf204a3f24.json', scope)
client = gspread.authorize(creds)

# Abrir a planilha e selecionar a worksheet
try:
    sheet = client.open('teste').sheet1
    print("Conectado à planilha do Google Sheets com sucesso!")
    
    # Converter o DataFrame para uma lista de listas e subir para o Google Sheets
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print("Dados carregados na planilha com sucesso!")
except Exception as e:
    print(f"Erro ao carregar dados no Google Sheets: {e}")
