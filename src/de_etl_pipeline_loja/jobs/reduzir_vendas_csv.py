import pandas as pd

caminho_csv = "src/de_etl_pipeline_loja/data/raw/vendas.csv"

# Lê o CSV original (3 milhões de linhas)
df = pd.read_csv(caminho_csv)

# Mantém apenas 300 mil linhas
df_reduzido = df.head(300_000)

# Sobrescreve o arquivo original com a versão reduzida
df_reduzido.to_csv(caminho_csv, index=False)