import requests
import pandas as pd
import os
import time
import boto3
from botocore.exceptions import NoCredentialsError

def upload_to_s3(local_file: str, bucket_name: str, s3_file_name: str):
    """
    Sobe um arquivo local para o bucket do Amazon S3.
    """
    s3 = boto3.client('s3')
    print(f"Iniciando upload para o S3: s3://{bucket_name}/{s3_file_name}")
    
    try:
        s3.upload_file(local_file, bucket_name, s3_file_name)
        print("✅ Upload concluído com sucesso!")
    except FileNotFoundError:
        print("❌ Arquivo não encontrado na máquina local.")
    except NoCredentialsError:
        print("❌ Credenciais da AWS não encontradas. Rode 'aws configure' no terminal.")
    except Exception as e:
        print(f"❌ Erro no upload: {e}")

def fetch_openmeteo_historical_full(lat: float, lon: float, start_year: int, end_year: int) -> pd.DataFrame:
    all_data = []
    
    todas_variaveis = (
        "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
        "precipitation,rain,surface_pressure,cloud_cover,visibility,"
        "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
        "soil_temperature_0cm,soil_moisture_0_to_7cm"
    )
    
    for year in range(start_year, end_year + 1):
        start_date = f"{year}-01-01"
        end_date = f"{year}-12-31" if year < 2026 else "2026-02-28"
        
        print(f"Extraindo histórico full de {start_date} a {end_date}...")
        
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
            response = requests.get(url, params=params, timeout=40)
            if response.status_code == 200:
                data = response.json()
                df = pd.DataFrame(data["hourly"])
                all_data.append(df)
                print(f" -> Sucesso: {len(df)} registros extraídos.")
            else:
                print(f" -> Erro no ano {year}: HTTP {response.status_code}")
        except Exception as e:
            print(f" -> Erro de conexão em {year}: {e}")
            
        time.sleep(1)
        
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    return pd.DataFrame()

def main():
    LAT, LON = -23.496, -46.620 # Mirante de Santana (A701)
    
    # ⚠️ COLOQUE O NOME DO SEU BUCKET AQUI ⚠️
    BUCKET_NAME = "smia-datalake-lpissaia"
    
    print("Iniciando Backfill Full via Open-Meteo...")
    print("-" * 60)
    
    df_raw = fetch_openmeteo_historical_full(LAT, LON, 2022, 2026)
    
    if not df_raw.empty:
        output_dir = "raw"
        os.makedirs(output_dir, exist_ok=True)
        
        file_name = "clima_backfill_full_openmeteo.parquet"
        file_path = os.path.join(output_dir, file_name)
        
        df_raw = df_raw.astype(str)
        df_raw.to_parquet(file_path, index=False)
        print("-" * 60)
        print(f"💾 Arquivo salvo localmente em: {file_path}")
        
        # O grande momento: Subindo para a Nuvem!
        # Vamos organizar em uma "pasta" dentro do bucket chamada open_meteo
        s3_key = f"open_meteo/{file_name}"
        upload_to_s3(file_path, BUCKET_NAME, s3_key)
        
    else:
        print("❌ Falha crítica no Backfill.")

if __name__ == "__main__":
    main()