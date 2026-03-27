'''Busca o valor do clima para o dia anterior (D-1) e salva na camada bronze.
'''

import requests
import pandas as pd
import boto3
from datetime import datetime, timedelta
import io

# --- 1. CREDENCIAIS AWS ---
AWS_ACCESS_KEY = "ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalakelpissaia"

# --- 2. AS 5 ZONAS DA NOSSA ARQUITETURA ---
ZONAS_SP = {
    "Centro": {"lat": -23.548, "lon": -46.636},
    "Norte":  {"lat": -23.486, "lon": -46.625},
    "Sul":    {"lat": -23.650, "lon": -46.700},
    "Leste":  {"lat": -23.540, "lon": -46.460},
    "Oeste":  {"lat": -23.550, "lon": -46.720}
}

def fetch_openmeteo_daily(zona: str, lat: float, lon: float, target_date: str) -> pd.DataFrame:
    print(f"Buscando clima da zona {zona.upper()} para o dia {target_date}...")
    
    # As mesmas variáveis do seu backfill
    todas_variaveis = (
        "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
        "precipitation,rain,surface_pressure,cloud_cover,visibility,"
        "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
        "soil_temperature_0cm,soil_moisture_0_to_7cm"
    )
    
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": target_date,
        "end_date": target_date,
        "hourly": todas_variaveis,
        "timezone": "America/Sao_Paulo"
    }
    
    try:
        response = requests.get(url, params=params, timeout=20)
        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data["hourly"])
            df = df.dropna(subset=["temperature_2m"])
            
            # 🔥 O PULO DO GATO: Adicionamos a coluna da zona!
            df['zona'] = zona
            return df
        else:
            print(f" ❌ Erro HTTP {response.status_code}")
    except Exception as e:
        print(f" ❌ Erro de conexão: {e}")
        
    return pd.DataFrame()

def main():
    # Pega o dia de ontem (D-1) automaticamente
    ontem = datetime.now() - timedelta(days=1)
    target_date = ontem.strftime("%Y-%m-%d")
    
    print(f"🤖 Iniciando Robô de Ingestão Incremental (Open-Meteo) | Referência: {target_date}")
    print("-" * 60)
    
    dfs_zonas = []
    
    # Loop para extrair as 5 zonas de SP
    for zona, coords in ZONAS_SP.items():
        df_zona = fetch_openmeteo_daily(zona, coords["lat"], coords["lon"], target_date)
        if not df_zona.empty:
            dfs_zonas.append(df_zona)
            
    if dfs_zonas:
        # Empilha as 5 zonas num único DataFrame
        df_incremental_final = pd.concat(dfs_zonas, ignore_index=True)
        df_incremental_final = df_incremental_final.astype(str)
        
        # Gera o arquivo final em memória e manda pro S3 direto (sem precisar do os.makedirs local)
        file_name = f"clima_incremental_openmeteo_{target_date}.parquet"
        s3_key = f"bronze/clima_incremental/{file_name}"
        
        print(f"\n📦 Preparando para salvar {len(df_incremental_final)} linhas no S3...")
        
        try:
            # Salvamento direto na nuvem usando o Pandas + fsspec
            df_incremental_final.to_parquet(
                f"s3://{BUCKET_NAME}/{s3_key}",
                index=False,
                storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
            )
            print(f"✅ SUCESSO! Carga incremental salva em: s3://{BUCKET_NAME}/{s3_key}")
        except Exception as e:
            print(f"❌ Erro ao salvar no S3: {e}")
            
    else:
        print("❌ Falha geral: Nenhum dado extraído de nenhuma zona.")

# Executa o código
main()