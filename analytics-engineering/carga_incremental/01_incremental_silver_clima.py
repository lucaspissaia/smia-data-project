'''Processo de Carga Incremental Diária do Clima para a Camada Silver'''


import pandas as pd
import pyspark.sql.functions as F
from datetime import datetime, timedelta

# --- 1. CREDENCIAIS AWS E CONFIGURAÇÕES ---
AWS_ACCESS_KEY = "ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalakelpissaia"

colunas_para_converter = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
    "precipitation", "rain", "surface_pressure", "cloud_cover", "visibility",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", 
    "soil_temperature_0cm", "soil_moisture_0_to_7cm"
]

print("⚙️ Iniciando Processamento Silver (Carga Incremental Diária do Clima)...")

# --- 2. DESCOBRINDO O ARQUIVO DE ONTEM NA BRONZE ---
ontem = datetime.now() - timedelta(days=1)
target_date = ontem.strftime("%Y-%m-%d")

arquivo_alvo = f"clima_incremental_openmeteo_{target_date}.parquet"
caminho_bronze = f"s3://{BUCKET_NAME}/bronze/clima_incremental/{arquivo_alvo}"

try:
    print(f"🔍 Buscando o arquivo: {arquivo_alvo}")
    
    # Bypass com Pandas
    df_pandas = pd.read_parquet(
        caminho_bronze,
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    df_spark = spark.createDataFrame(df_pandas)
    
    # --- 3. TRANSFORMANDO COM O MESMO SCHEMA DO BACKFILL ---
    print("🛠️ Aplicando tipagem e agregações diárias (Padrão Ouro)...")
    
    # Tipagem
    df_clima_tipado = df_spark.withColumn("data_clima", F.to_date(F.substring(F.col("time"), 1, 10)))
    for col in colunas_para_converter:
        df_clima_tipado = df_clima_tipado.withColumn(col, F.col(col).cast("double"))
        
    # Agregação idêntica ao seu histórico
    df_clima_diario = df_clima_tipado.groupBy("data_clima", "zona").agg(
        F.max("temperature_2m").alias("temp_maxima"),
        F.min("temperature_2m").alias("temp_minima"),
        F.round(F.avg("temperature_2m"), 2).alias("temp_media"),
        F.round(F.avg("apparent_temperature"), 2).alias("sensacao_termica_media"),
        F.round(F.avg("dew_point_2m"), 2).alias("ponto_orvalho_medio"),
        F.round(F.sum("precipitation"), 2).alias("precipitacao_total"),
        F.round(F.sum("rain"), 2).alias("chuva_total"),
        F.round(F.avg("relative_humidity_2m"), 2).alias("umidade_media_pct"),
        F.round(F.avg("surface_pressure"), 2).alias("pressao_superficie_media"),
        F.round(F.avg("wind_speed_10m"), 2).alias("velocidade_vento_media"),
        F.max("wind_gusts_10m").alias("rajada_vento_maxima"),
        F.round(F.avg("wind_direction_10m"), 2).alias("direcao_vento_media"),
        F.round(F.avg("cloud_cover"), 2).alias("cobertura_nuvens_media"),
        F.round(F.avg("visibility"), 2).alias("visibilidade_media"),
        F.round(F.avg("soil_temperature_0cm"), 2).alias("temp_solo_media"),
        F.round(F.avg("soil_moisture_0_to_7cm"), 4).alias("umidade_solo_media")
    )
    
    # Limpeza de colunas nulas (como no original)
    df_clima_limpo = df_clima_diario.drop("visibilidade_media", "temp_solo_media")
    
    # --- 4. SALVANDO O APPEND NO S3 ---
    nome_arquivo_novo = f"clima_silver_sp_{target_date}.parquet"
    caminho_silver = f"s3://{BUCKET_NAME}/silver/clima/{nome_arquivo_novo}"
    
    print(f"💾 Salvando o novo dia no Data Lake: {nome_arquivo_novo}")
    
    linhas_python = [row.asDict() for row in df_clima_limpo.collect()]
    df_final_pandas = pd.DataFrame(linhas_python)

    df_final_pandas.to_parquet(
        caminho_silver,
        index=False,
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    print(f"✅ SUCESSO ABSOLUTO! Arquivo salvo em: {caminho_silver}")

except Exception as e:
    print(f"❌ Erro na carga incremental: {e}")