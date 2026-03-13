import requests
import pandas as pd
import os
import time

def fetch_openmeteo_historical_full(lat: float, lon: float, start_year: int, end_year: int) -> pd.DataFrame:
    """
    Busca o histórico do Open-Meteo pedindo todas as variáveis relevantes para a Camada Bronze.
    """
    all_data = []
    
    # Lista massiva de variáveis para o nosso Data Lake (True ELT)
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
                
                # MÁGICA DINÂMICA: Transforma todo o bloco 'hourly' num DataFrame automático.
                # Não importa quantas variáveis pedimos, ele cria a coluna com o nome original.
                df = pd.DataFrame(data["hourly"])
                
                all_data.append(df)
                print(f" -> Sucesso: {len(df)} registros com {len(df.columns)} colunas extraídos.")
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
    
    print("Iniciando Backfill Full (Camada Bronze) via Open-Meteo...")
    print("-" * 60)
    
    df_raw = fetch_openmeteo_historical_full(LAT, LON, 2022, 2026)
    
    if not df_raw.empty:
        output_dir = "raw"
        os.makedirs(output_dir, exist_ok=True)
        
        file_path = os.path.join(output_dir, "clima_backfill_full_openmeteo.parquet")
        
        df_raw = df_raw.astype(str)
        df_raw.to_parquet(file_path, index=False)
        
        print("-" * 60)
        print(f"✅ Arquivo Bronze guardado com sucesso: {file_path}")
        print(f"📊 Total de registros (horas): {len(df_raw)}")
        print(f"📊 Total de colunas brutas preservadas: {len(df_raw.columns)}")
    else:
        print("❌ Falha crítica no Backfill.")

if __name__ == "__main__":
    main()