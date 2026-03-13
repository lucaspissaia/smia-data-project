#USAR NO GOOGLE COLAB OU AMBIENTE LINUX

import os
import gc # Garbage Collector (O faxineiro da memória RAM)
import pandas as pd
from pysus import SINAN
import boto3

# --- SUAS CREDENCIAIS AWS ---
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

def main():
    CODIGO_IBGE_SP = "35"
    anos_para_baixar = [2022, 2023, 2024, 2025, 2026]

    print("Iniciando extração do DataSUS com Particionamento (Memory Safe)...")
    print("-" * 60)

    sinan = SINAN().load()
    output_dir = "/content/raw"
    os.makedirs(output_dir, exist_ok=True)

    for ano in anos_para_baixar:
        print(f"\n🚀 Iniciando processamento do ano: {ano}")

        # Pede os arquivos apenas daquele ano específico
        arquivos_ftp = sinan.get_files(dis_code="DENG", year=[ano])

        for arquivo in arquivos_ftp:
            try:
                print(f" -> Baixando arquivo do governo: {arquivo.name}...")
                df_br = arquivo.download().to_dataframe()

                if 'SG_UF_NOT' in df_br.columns:
                    # O .copy() é importante para desvincular o pedaço da memória principal
                    df_uf = df_br[df_br['SG_UF_NOT'] == CODIGO_IBGE_SP].copy()

                    if not df_uf.empty:
                        file_name = f"sinan_dengue_SP_{ano}.parquet"
                        file_path = os.path.join(output_dir, file_name)

                        df_uf = df_uf.astype(str)
                        df_uf.to_parquet(file_path, index=False)
                        print(f" -> 💾 Salvo em Parquet: {len(df_uf)} registros.")

                        s3_key = f"datasus/{file_name}"
                        upload_to_s3(file_path, BUCKET_NAME, s3_key)
                    else:
                        print(f" -> ⚠️ Sem registros de SP no arquivo {arquivo.name}.")

                # A MÁGICA ACONTECE AQUI: Deletar as variáveis gigantes da memória
                del df_br
                if 'df_uf' in locals():
                    del df_uf

                # Chama o caminhão de lixo do Python para liberar a RAM fisicamente
                gc.collect()
                print(" -> 🧹 RAM limpa para o próximo arquivo.")

            except Exception as e:
                print(f" -> ❌ Erro ao processar o arquivo {arquivo.name}: {e}")

if __name__ == "__main__":
    main()