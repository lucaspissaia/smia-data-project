import requests
import pandas as pd
import os
import boto3
from datetime import datetime, timedelta

# --- CREDENCIAIS AWS ---
# Em produção (Lambda), não precisamos dessas chaves, mas para rodar local é necessário.
AWS_ACCESS_KEY = "COLE_SUA_ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "COLE_SUA_SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalake-lpissaia"

def upload_to_s3(local_file: str, bucket_name: str, s3_file_name: str):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    print(f" -> Subindo para o S3: s3://{bucket_name}/{s3_file_name}")
    try:
        s3.upload_file(local_file, bucket_name, s3_file_name)
        print("   ✅ Upload concluído!")
    except Exception as e:
        print(f"   ❌ Erro no upload: {e}")

def fetch_openmeteo_daily(lat: float, lon: float, target_date: str) -> pd.DataFrame:
    print(f"A extrair dados incrementais para o dia {target_date}...")
    
    # As mesmas variáveis do backfill (True ELT)
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
            # Remove linhas onde as métricas ainda não foram consolidadas pelas estações
            df = df.dropna(subset=["temperature_2m"])
            print(f" -> Sucesso: {len(df)} registros extraídos.")
            return df
        else:
            print(f" -> Erro na requisição: HTTP {response.status_code}")
    except Exception as e:
        print(f" -> Erro de conexão: {e}")
        
    return pd.DataFrame()

def main():
    LAT, LON = -23.496, -46.620 # Mirante de Santana (A701)
    
    # D-1 (Ontem)
    ontem = datetime.now() - timedelta(days=1)
    target_date = ontem.strftime("%Y-%m-%d")
    
    print("Iniciando Carga Incremental (Open-Meteo)")
    print("-" * 60)
    
    df_daily = fetch_openmeteo_daily(LAT, LON, target_date)
    
    if not df_daily.empty:
        output_dir = "raw"
        os.makedirs(output_dir, exist_ok=True)
        
        file_name = f"clima_incremental_openmeteo_{target_date}.parquet"
        file_path = os.path.join(output_dir, file_name)
        
        df_daily = df_daily.astype(str)
        df_daily.to_parquet(file_path, index=False)
        
        # Sobe para o S3
        s3_key = f"open_meteo/{file_name}"
        upload_to_s3(file_path, BUCKET_NAME, s3_key)
    else:
        print("❌ Falha na extração diária de clima.")

if __name__ == "__main__":
    main()