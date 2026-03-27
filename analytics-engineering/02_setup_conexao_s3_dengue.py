'''DENGUE
Bloco abaixo é para buscar os dados brutos que estão em um bucket no s3, combinar com a tabela de CNES e aplicar as primeiras transformações necessárias e enviar novamente para uma pasta "silver" no mesmo bucket. 
Transformações aplicadas: 
- filtrar apenas moradores da cidade de São Paulo;
- Confirmar formato da data;
- Transformar coluna de idade;
- Conexão com tabela CNES; 
- Mapear códigos CNES e transformar em zonas;
- Left join com tabela de CNES para trazer zona, nome do posto e bairro;
- Enviar novamente ao s3;
- Testar o código SQL que vai ser usado na camada gold.
'''

import pandas as pd
import pyspark.sql.functions as F

# --- BLOCO 01: CONFIGURAÇÕES E LEITURA DO DENGUE DO S3 (CAMADA BRONZE)
# --- 1. CHAVES DA AWS ---
AWS_ACCESS_KEY = "ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalakelpissaia"

# Puxar o arquivo de 2025 gerado no Colab
CAMINHO_BRONZE_DENGUE = "datasus/sinan_dengue_SP_2025.parquet"

print("🔄 Puxando os dados da Dengue (SINAN 2025) do S3...")

try:
    # 2. Leitura via Pandas (Bypass de Segurança)
    df_dengue_pandas = pd.read_parquet(
        f"s3://{BUCKET_NAME}/{CAMINHO_BRONZE_DENGUE}",
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    
    # 3. Conversão para PySpark para a faxina pesada
    df_dengue_spark = spark.createDataFrame(df_dengue_pandas)
    
    print(f"✅ Arquivo carregado! Total de registros brutos em SP (Estado): {df_dengue_spark.count()}")
    
    # 4. Mostra as primeiras linhas pra gente começar a desenhar a limpeza
    display(df_dengue_spark)
    display(df_dengue_spark.limit(1))
    
except Exception as e:
    print(f"❌ Erro crítico ao ler a Dengue: {e}")


# --- BLOCO 02: LIMPEZA E TRANSFORMAÇÃO PARA A CAMADA SILVER

print("🛠️ Refazendo a Faxina Silver: Preservando a riqueza clínica e estatística...")

# 1. Filtro Espacial: Manter APENAS moradores de São Paulo Capital
df_dengue_sp = df_dengue_spark.filter(F.col("ID_MN_RESI") == "355030")

# 2. Transformações Mágicas (Adicionando colunas novas sem destruir as antigas)
df_dengue_silver = df_dengue_sp.withColumn(
    "data_tratada", F.to_date(F.col("DT_NOTIFIC"))
).withColumn(
    "idade_tratada",
    F.when(F.substring(F.col("NU_IDADE_N"), 1, 1) == "4",
           F.substring(F.col("NU_IDADE_N"), 2, 3).cast("int"))
     .otherwise(0)
)

print(f"📊 Faxina concluída! Linhas retidas na Capital: {df_dengue_silver.count()}")
print(f"🧬 Total de colunas preservadas para a Inteligência Artificial: {len(df_dengue_silver.columns)}")
print("✅ A tabela com as novas colunas:")

display(df_dengue_silver)


# --- BLOCO 03: TABELA DIMENSÃO (CNES), JOIN E TRATAMENTO DE NULOS ---
print("🔍 Lendo a Tabela Dimensão (CNES) do S3...")

try:
    # 1. Leitura do CNES (com encoding latin1 para não dar erro nos acentos)
    df_cnes_pandas = pd.read_csv(
        f"s3://{BUCKET_NAME}/raw/cnes_sp.csv", 
        sep=';', 
        dtype=str,
        encoding='latin1',
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    df_cnes_spark = spark.createDataFrame(df_cnes_pandas)
    
    # 2. Preparando a Dimensão (Filtro SP + Regra do CEP)
    print("🛠️ Tratando os códigos CNES e mapeando Zonas pelo CEP...")
    df_cnes_dimensao = df_cnes_spark.filter(F.col("CO_IBGE") == "355030").withColumn(
        "ID_UNIDADE_JOIN", F.lpad(F.col("CO_CNES"), 7, '0')
    ).withColumn(
        "prefixo_cep", F.substring(F.col("CO_CEP"), 1, 2)
    ).withColumn(
        "zona_oficial",
        F.when(F.col("prefixo_cep").isin("01", "03"), "Centro")
         .when(F.col("prefixo_cep") == "02", "Norte")
         .when(F.col("prefixo_cep") == "04", "Sul")
         .when(F.col("prefixo_cep") == "05", "Oeste")
         .when(F.col("prefixo_cep").isin("08", "09"), "Leste")
         .otherwise("Desconhecida")
    ).select("ID_UNIDADE_JOIN", "zona_oficial", "NO_FANTASIA", "NO_BAIRRO")
    
    # 3. Junção das tabelas (LEFT JOIN)
    print("🤝 Cruzando a base de Dengue com os Hospitais Reais...")
    df_dengue_enriquecida = df_dengue_silver.join(
        df_cnes_dimensao,
        df_dengue_silver["ID_UNIDADE"] == df_cnes_dimensao["ID_UNIDADE_JOIN"],
        "left"
    )
    
    # 4. Tratamento dos Nulos (Usando a lógica direta)
    df_dengue_silver_final = df_dengue_enriquecida.withColumn(
        "zona", F.coalesce(F.col("zona_oficial"), F.lit("Zona Nao Identificada"))
    ).withColumn(
        "NO_FANTASIA", F.coalesce(F.col("NO_FANTASIA"), F.lit("POSTO NAO IDENTIFICADO NO CNES"))
    ).withColumn(
        "NO_BAIRRO", F.coalesce(F.col("NO_BAIRRO"), F.lit("BAIRRO NAO IDENTIFICADO"))
    ).drop("ID_UNIDADE_JOIN", "zona_oficial")
    
    # 5. Relatório de Qualidade na Tela
    print("📊 RELATÓRIO DE QUALIDADE: Volume de dados por Zona")
    df_dengue_silver_final.groupBy("zona").count().orderBy("count", ascending=False).show()

except Exception as e:
    print(f"❌ Erro ao processar o cruzamento: {e}")


# --- BLOCO 04: SALVANDO O RESULTADO NO S3 ---
print("💾 Extraindo dados limpos para salvar (Bypass do Serverless)...")

try:
    # Coletando do Spark para Pandas
    linhas_python = [row.asDict() for row in df_dengue_silver_final.collect()]
    df_final_pandas = pd.DataFrame(linhas_python)

    caminho_silver_dengue = f"s3://{BUCKET_NAME}/silver/dengue/dengue_enriquecida_sp_2025.parquet"
    print("🚀 Enviando a Tabela Oficial de Dengue para a AWS...")

    df_final_pandas.to_parquet(
        caminho_silver_dengue,
        index=False,
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    print(f"✅ SUCESSO! Tabela limpa e salva em: {caminho_silver_dengue}")

except Exception as e:
    print(f"❌ Erro ao salvar no S3: {e}")



# --- BLOCO 05: TESTE RÁPIDO DO SQL PARA A CAMADA GOLD ---


print("🥇 Iniciando a materialização da Camada Gold...")

# 1. Rodar a query SQL e guardar o resultado numa variável
query_gold = """
SELECT 
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
    
    c.temp_maxima,
    c.temp_minima,
    c.temp_media,
    c.precipitacao_total AS chuva_mm,
    c.umidade_media_pct AS umidade
    
FROM vw_dengue d
LEFT JOIN vw_clima c
    ON d.data_tratada = c.data_clima 
    AND d.zona = c.zona
"""

# Executar o SQL no motor do Spark
df_gold_spark = spark.sql(query_gold)

print("💾 Extraindo a Tabela Fato para salvar no S3...")

try:
    # 2. O Bypass seguro para Pandas
    linhas_python = [row.asDict() for row in df_gold_spark.collect()]
    df_gold_pandas = pd.DataFrame(linhas_python)

    # 3. O Destino Final: Camada Gold!
    caminho_gold = f"s3://{BUCKET}/gold/fato_dengue_clima_2025.parquet"
    
    print("🚀 Enviando para a AWS...")

    df_gold_pandas.to_parquet(
        caminho_gold,
        index=False,
        storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
    )
    print(f"✅ SUCESSO ABSOLUTO! Tabela Fato salva em: {caminho_gold}")
    print("🏆 PIPELINE DE ENGENHARIA DE DADOS (MVP) CONCLUÍDO!")

except Exception as e:
    print(f"❌ Erro ao salvar a Gold no S3: {e}")