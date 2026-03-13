#extração incremental diária do Open-Meteo a ser configurada envio automático a nuvem

import requests
import pandas as pd
import os
from datetime import datetime, timedelta

def fetch_openmeteo_daily(lat: float, lon: float, target_date: str) -> pd.DataFrame:
    """
    Busca os dados meteorológicos de um dia específico (D-1).
    """
    # Lista massiva de variáveis para o nosso Data Lake (True ELT)
    todas_variaveis = (
        "temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
        "precipitation,rain,surface_pressure,cloud_cover,visibility,"
        "wind_speed_10m,wind_direction_10m,wind_gusts_10m,"
        "soil_temperature_0cm,soil_moisture_0_to_7cm"
    )

    print(f"A extrair dados incrementais para o dia {target_date}...")
    
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
            
            print(f" -> Sucesso: {len(df)} registos horários extraídos.")
            return df
        else:
            print(f" -> Erro na requisição: HTTP {response.status_code}")
            return pd.DataFrame()
            
    except Exception as e:
        print(f" -> Erro de conexão: {e}")
        return pd.DataFrame()

def main():
    LAT, LON = -23.496, -46.620 # Mirante de Santana
    
    # Definir extração para D-1 (Ontem)
    ontem = datetime.now() - timedelta(days=1)
    target_date = ontem.strftime("%Y-%m-%d")
    
    print("A iniciar Carga Incremental (Open-Meteo)")
    print("-" * 60)
    
    df_daily = fetch_openmeteo_daily(LAT, LON, target_date)
    
    if not df_daily.empty:
        output_dir = "raw"
        os.makedirs(output_dir, exist_ok=True)
        
        file_path = os.path.join(output_dir, f"clima_incremental_openmeteo_{target_date}.parquet")
        
        df_daily = df_daily.astype(str)
        df_daily.to_parquet(file_path, index=False)
        
        print("-" * 60)
        print(f"✅ Ficheiro incremental guardado com sucesso: {file_path}")
    else:
        print(f"❌ Falha: Não foi possível extrair a carga para {target_date}.")

if __name__ == "__main__":
    main()