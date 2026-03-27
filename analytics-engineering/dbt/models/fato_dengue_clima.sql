'''
Leitura da tabela fato_dengue_clima criada no dbt.
'''


{{ config(materialized='table') }}

WITH dengue AS (
    SELECT * FROM workspace.default.silver_dengue
),

clima AS (
    SELECT * FROM workspace.default.silver_clima
)

SELECT 
    d.ID_UNIDADE,
    d.NO_FANTASIA AS nome_posto,
    d.NO_BAIRRO AS bairro_posto,
    d.zona,
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
    
FROM dengue d
LEFT JOIN clima c
    ON d.data_tratada = c.data_clima 
    AND d.zona = c.zona