'''CLIMA
Bloco abaixo é para buscar os dados brutos que estão em um bucket no s3, aplicar as primeiras transformações necessárias e enviar novamente para uma pasta "silver" no mesmo bucket. Transformações aplicadas:

os dados estavam de hora em hora, é necessário criar o resumo diário;
valores máximos, mínimos e médios de acordo com o indicador;
drop de duas colunas 100% nulas;'''

import pandas as pd
import pyspark.sql.functions as F

# --- 1. CONFIGURAÇÕES E CHAVES ---
AWS_ACCESS_KEY = "ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalakelpissaia"

ZONAS = ["centro", "norte", "sul", "leste", "oeste"]

colunas_para_converter = [
    "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
    "precipitation", "rain", "surface_pressure", "cloud_cover", "visibility",
    "wind_speed_10m", "wind_direction_10m", "wind_gusts_10m", 
    "soil_temperature_0cm", "soil_moisture_0_to_7cm"
]

print("🚀 Iniciando Processamento em Lote: Camada Silver (Clima)")
print("=" * 60)

# --- 2. LOOP PRINCIPAL ---
for zona in ZONAS:
    print(f"\n🔄 Processando Zona: {zona.upper()}...")
    
    caminho_bronze = f"open_meteo/clima_backfill_full_SP_{zona}.parquet"
    caminho_silver = f"s3://{BUCKET_NAME}/silver/clima/clima_diario_{zona}.parquet"
    
    try:
        # A. Leitura Segura do S3 via Pandas
        df_pandas = pd.read_parquet(
            f"s3://{BUCKET_NAME}/{caminho_bronze}",
            storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
        )
        
        # B. Conversão para o motor PySpark
        df_spark = spark.createDataFrame(df_pandas)
        
        # C. Transformação e Tipagem
        df_clima_tipado = df_spark.withColumn("data_clima", F.to_date(F.substring(F.col("time"), 1, 10)))
        
        for col in colunas_para_converter:
            df_clima_tipado = df_clima_tipado.withColumn(col, F.col(col).cast("double"))
            
        # D. Agregação Diária
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
        ).orderBy("data_clima")
        
        # E. Limpeza das Colunas Nulas
        df_clima_limpo = df_clima_diario.drop("visibilidade_media", "temp_solo_media")
        
        # F. Bypass do Bug Serverless e Conversão de volta para Pandas
        linhas_python = [row.asDict() for row in df_clima_limpo.collect()]
        df_final_pandas = pd.DataFrame(linhas_python)
        
        # G. Escrita na Camada Silver no S3
        df_final_pandas.to_parquet(
            caminho_silver,
            index=False,
            storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
        )
        
        print(f"   ✅ SUCESSO! Salvo em: silver/clima/clima_diario_{zona}.parquet")
        
    except Exception as e:
        print(f"   ❌ Erro crítico na zona {zona.upper()}: {e}")

print("\n" + "=" * 60)
print("🏆 PIPELINE SILVER DO CLIMA CONCLUÍDO COM SUCESSO!")