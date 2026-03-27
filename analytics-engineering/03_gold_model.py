'''Agora que temos as tabelas Silver de Dengue e Clima prontas, vamos criar a nossa Tabela Fato na Camada Gold.
Juntando as informações clínicas dos pacientes com os dados meteorológicos diários.
'''


import pandas as pd

# --- BLOCO 01: CONFIGURAÇÕES E LEITURA DAS TABELAS SILVER DO S3 ---
# --- 0. CONFIGURAÇÕES E CHAVES ---
AWS_ACCESS_KEY = "ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalakelpissaia"

print("🔄 Trazendo as tabelas Silver do S3 para a memória...")

# 1. Lendo a pasta inteira de Clima (Todas as Zonas e Anos)
df_clima = pd.read_parquet(
    f"s3://{BUCKET}/silver/clima/",
    storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
)
# Transforma em PySpark e cria uma "Tabela Virtual" chamada vw_clima
spark.createDataFrame(df_clima).createOrReplaceTempView("vw_clima")

# 2. Lendo a Dengue de 2025 -> passar para a pasta inteira quando resolver o problema de 2024
df_dengue = pd.read_parquet(
    f"s3://{BUCKET}/silver/dengue/dengue_enriquecida_sp_2025.parquet",
    storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
)
# Transforma em PySpark e cria uma "Tabela Virtual" chamada vw_dengue
spark.createDataFrame(df_dengue).createOrReplaceTempView("vw_dengue")

print("✅ Views criadas com sucesso! O palco está pronto para o SQL.")

# --- BLOCO 02: SQL DE JUNÇÃO E TRANSFORMAÇÃO PARA A TABELA FATO GOLD NO DBT---
'''%sql
-- Este é EXATAMENTE o código que vai morar no seu dbt no futuro!

SELECT 
    -- Dados do Paciente e Localização (Vem da Dengue)
    d.ID_UNIDADE,
    d.NO_FANTASIA AS nome_posto,
    d.NO_BAIRRO AS bairro_posto,
    d.zona AS zona,
    d.data_tratada AS data_notificacao,
    d.idade_tratada AS idade,
    d.CS_SEXO AS sexo,
    d.FEBRE, 
    d.MIALGIA,
    d.CLASSI_FIN AS diagnostico_final,
    
    -- Dados Meteorológicos do EXATO dia e zona (Vem do Clima)
    c.temp_maxima,
    c.temp_minima,
    c.temp_media,
    c.precipitacao_total AS chuva_mm,
    c.umidade_media_pct AS umidade
    
FROM vw_dengue d
LEFT JOIN vw_clima c
    ON d.data_tratada = c.data_clima 
    AND d.zona = c.zona
    
-- Vamos ordenar para ver os primeiros dias do ano
ORDER BY d.data_tratada ASC
'''


# --- BLOCO 03: EXTRAÇÃO DO RESULTADO DO SQL E ENVIO PARA O S3 ---

AWS_ACCESS_KEY = "ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalakelpissaia"

print("🌉 Construindo a Ponte para o dbt...")

try:
    # 1. Puxando a Silver Dengue do S3
    df_dengue = pd.read_parquet(
        f"s3://{BUCKET}/silver/dengue/dengue_enriquecida_sp_2025.parquet",
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    # Salvando DENTRO do Databricks (Schema default)
    spark.createDataFrame(df_dengue).write.mode("overwrite").saveAsTable("default.silver_dengue")
    print("✅ Tabela silver_dengue registrada no Databricks!")

    # 2. Puxando a Silver Clima do S3
    df_clima = pd.read_parquet(
        f"s3://{BUCKET}/silver/clima/",
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    # Salvando DENTRO do Databricks (Schema default)
    spark.createDataFrame(df_clima).write.mode("overwrite").saveAsTable("default.silver_clima")
    print("✅ Tabela silver_clima registrada no Databricks!")
    
    print("🚀 O dbt já pode enxergar os seus dados!")

except Exception as e:
    print(f"❌ Erro ao criar a ponte: {e}")