import yfinance as yf
import pandas as pd
from google.cloud import bigquery
import time

# --- 1. CONFIGURACIÓN ---
RUTA_JSON_GOOGLE = 'C:/Users/marco/Downloads/logistics-metrics-488014-d5a9a9c0efe5.json' 
PROJECT_ID = "logistics-metrics-488014"
DATASET_ID = "adr_argentina" 
TABLE_ID = "raw_adr_history"


# 15 ADRs Argentinos principales
ADR_TICKERS = [
    "GGAL", "YPF", "MELI", "BMA", "PAM", 
    "TGS", "GLOB", "TEO", "CEPU", "EDN",
    "BBAR", "IRS", "LOMA", "SUPV", "CRESY"
]

client = bigquery.Client.from_service_account_json(RUTA_JSON_GOOGLE)

def fetch_full_adr_data(tickers):
    print(f"Descargando historial completo (3 años) para {len(tickers)} activos...")
    
    # Descargamos TODOS los campos (Open, High, Low, Close, Volume)
    # auto_adjust=True asegura que los precios reflejen dividendos y splits
    df = yf.download(tickers, period="3y", interval="1d", auto_adjust=True)
    
    # Reestructuramos el DataFrame: pasamos los Tickers de columnas a filas
    # Esto crea una tabla larga (Long Format), que es la mejor práctica en SQL
    df = df.stack(level=1).reset_index()
    
    # Limpiamos los nombres de las columnas para BigQuery (quitar espacios y minúsculas)
    df.columns = [c.replace(' ', '_').lower() for c in df.columns]
    
    # Renombramos 'level_1' (o como se llame la columna del ticker) a 'ticker'
    # yfinance suele llamarla 'Ticker' o 'level_1' dependiendo de la versión
    if 'level_1' in df.columns:
        df = df.rename(columns={'level_1': 'ticker'})
    elif 'Ticker' in df.columns:
        df = df.rename(columns={'Ticker': 'ticker'})

    # Aseguramos que la columna de fecha sea tipo datetime
    df['date'] = pd.to_datetime(df['date']).dt.date
    
    # Eliminamos filas donde no hay precio (feriados)
    df = df.dropna(subset=['close'])
    
    return df

# --- 2. EJECUCIÓN ---
try:
    df_history = fetch_full_adr_data(ADR_TICKERS)
    print(f"Éxito: Se obtuvieron {len(df_history)} registros con todas las métricas.")
    
    # --- 3. CARGA A BIGQUERY ---
    if not df_history.empty:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True # Detectará automáticamente Close, Open, High, Low, Volume
        )

        print(f"Subiendo a BigQuery: {table_ref}...")
        job = client.load_table_from_dataframe(df_history, table_ref, job_config=job_config)
        job.result()
        
        print("\n" + "="*30)
        print("¡CARGA COMPLETADA EXITOSAMENTE!")
        print(f"Tabla: {TABLE_ID}")
        print(f"Columnas cargadas: {list(df_history.columns)}")
        print("="*30)

except Exception as e:
    print(f"Error durante el proceso: {e}")
