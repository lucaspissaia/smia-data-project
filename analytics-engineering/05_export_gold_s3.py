'''Para enviar ao s3, a título de portfólio e visualização. A tabela gold vai continuar sendo acessada pelo databricks
'''

import pandas as pd

# Suas chaves
AWS_ACCESS_KEY = "ACCESS_KEY_AQUI"
AWS_SECRET_KEY = "SECRET_KEY_AQUI"
BUCKET_NAME = "smia-datalakelpissaia"

print("📤 Exportando a Tabela Gold do Databricks para o seu Data Lake S3...")

# 1. Lê a tabela gerenciada que o dbt acabou de criar
df_gold_spark = spark.table("workspace.default.fato_dengue_clima")

# 2. Converte e Salva no seu S3
linhas_python = [row.asDict() for row in df_gold_spark.collect()]
df_gold_pandas = pd.DataFrame(linhas_python)

caminho_gold = f"s3://{BUCKET}/gold/fato_dengue_clima_2025.parquet"

df_gold_pandas.to_parquet(
    caminho_gold,
    index=False,
    storage_options={"key": AWS_ACCESS_KEY, "secret": AWS_SECRET_KEY}
)
print(f"✅ SUCESSO! Cópia física salva em: {caminho_gold}")