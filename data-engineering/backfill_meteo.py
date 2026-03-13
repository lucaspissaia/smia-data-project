import requests
import pandas as pd
import os
import time
import boto3
import gc
from botocore.exceptions import NoCredentialsError

# --- CONFIGURAÇÃO AWS ---
AWS_ACCESS_KEY = "COLE_SUA_ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "COLE_SUA_SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalake-lpissaia"

# --- DICIONÁRIO DE COORDENADAS (Pontos Centrais das Zonas de SP) ---
ZONAS_SP = {
    "Centro": {"lat": -23.5505, "lon": -46.6333},
    "Norte":  {"lat": -23.4981, "lon": -46.6251},
    "Sul":    {"lat": -23.6500, "lon": -46.7000},
    "Leste":  {"lat": -23.5500, "lon": -46.4600},
    "Oeste":  {"lat": -23.5329, "lon": -46.7118}
}

def upload_to_s3(local_file: str, bucket_name: str, s3_file_name: str):
    """Sobe arquivo para o S3 com chaves explícitas (necessário no Colab)."""
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    print(f"   -> Subindo para S3: s3://{bucket_name}/{s3_file_name}")
    try:
        s3.upload_file(local_file, bucket_name, s3_file_name)
        print("      ✅ Upload concluído!")
    except Exception as e:
        print(f"      ❌ Erro no upload: {e}")

def fetch_openmeteo_historical_zone(lat: float, lon: float, start_year: int, end_year: int, nome_zona: str) -> pd.DataFrame:
    """Baixa o histórico de uma coordenada específica e adiciona a coluna da zona."""
    all_data = []
    
    todas_variaveis = (
        "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
        "precipitation,rain,surface_pressure,cloud_cover,visibility,"
        "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
        "soil_temperature_0cm,soil_moisture_0_to_7cm"
    )
    
    # Loop de anos (2022 a 2026) para evitar estourar a RAM da API
    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        # Ajuste para fechar em Fev/2026 se for o ano atual
        end_date = f"{year}-12-31" if year < 2026 else "2026-02-28"
        
        print(f"   -> Extraindo {year} para Zona {nome_zona}...")
        
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": todas_variaveis,
            "timezone": "America/Sao_Paulo"
        }
        
        try:
            # Aumentado timeout pois são muitas variáveis
            response = requests.get(url, params=params, timeout=60) 
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data["hourly"])
                # Remove linhas sem dados (garante integridade)
                df = df.dropna(subset=["temperature_2m"]) 
                all_data.append(df)
            else:
                print(f"      ⚠️ Erro no ano {year}: HTTP {response.status_code}")
        except Exception as e:
            print(f"      ⚠️ Erro de conexão em {year}: {e}")
            
        # Pequena pausa para ser gentil com a API gratuita
        time.sleep(1) 
        
    if all_data:
        df_final = pd.concat(all_data, ignore_index=True)
        # ADICIONA A CHAVE GEOGRÁFICA
        df_final['zona'] = nome_zona 
        return df_final
    return pd.DataFrame()

def main():
    print("Iniciando Backfill Multizonal Open-Meteo (Memory Safe)")
    print("=" * 60)
    
    output_dir = "/content/raw"
    os.makedirs(output_dir, exist_ok=True)
    
    # Loop principal pelas 5 Zonas
    for nome_zona, coords in ZONAS_SP.items():
        print(f"\n🚀 Processando Zona: {nome_zona.upper()}")
        print("-" * 40)
        
        # Baixa histórico completo (2022-2026) com a coluna 'zona' já inclusa
        df_raw = fetch_openmeteo_historical_zone(coords["lat"], coords["lon"], 2022, 2026, nome_zona)
        
        if not df_raw.empty:
            file_name = f"clima_backfill_full_SP_{nome_zona.lower()}.parquet"
            file_path = os.path.join(output_dir, file_name)
            
            # True ELT: Mantém tudo como string na Bronze para não quebrar schema
            df_raw = df_raw.astype(str) 
            df_raw.to_parquet(file_path, index=False)
            print(f"   -> 💾 Salvo localmente: {len(df_raw)} registros horários.")
            
            # Sobe para a pasta open_meteo/ no S3
            s3_key = f"open_meteo/{file_name}"
            upload_to_s3(file_path, BUCKET_NAME, s3_key)
        else:
            print(f"   ❌ Falha crítica: Sem dados para a Zona {nome_zona}.")
            
        # MÁGICA DA MEMÓRIA: Deleta os DataFrames pesados e chama o faxineiro da RAM
        if 'df_raw' in locals():
            del df_raw
        gc.collect() 
        print("   -> 🧹 RAM limpa para a próxima zona.")

    print("\n" + "="*60)
    print("🏆 Processamento Multizonal Concluído com Sucesso!")

if __name__ == "__main__":
    main()