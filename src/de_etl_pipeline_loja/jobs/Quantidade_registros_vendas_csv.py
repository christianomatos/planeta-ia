
import pandas as pd

caminho_csv = "src/de_etl_pipeline_loja/data/raw/vendas.csv"

df = pd.read_csv(caminho_csv)

print("Quantidade de linhas:", len(df))
# ou
print("Quantidade de linhas (shape):", df.shape[0])