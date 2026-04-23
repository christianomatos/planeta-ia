from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
BRONZE_DIR = BASE_DIR / "data" / "bronze"
SILVER_DIR = BASE_DIR / "data" / "silver"
SILVER_DIR.mkdir(parents=True, exist_ok=True)

def load_bronze_tables():
    clientes = pd.read_parquet(BRONZE_DIR / "clientes.parquet")
    produtos = pd.read_parquet(BRONZE_DIR / "produtos.parquet")
    vendas = pd.read_parquet(BRONZE_DIR / "vendas.parquet")
    return clientes, produtos, vendas

def clean_dim_clientes(clientes: pd.DataFrame) -> pd.DataFrame:
    # Renomeia colunas para um padrão consistente
    df = clientes.rename(
        columns={
            "id": "cliente_id",
            "nome": "cliente_nome",
            "email": "cliente_email",
            "criado_em": "cliente_criado_em",
        }
    )

    # Garante tipo datetime
    if "cliente_criado_em" in df.columns:
        df["cliente_criado_em"] = pd.to_datetime(df["cliente_criado_em"])

    return df

def clean_dim_produtos(produtos: pd.DataFrame) -> pd.DataFrame:
    df = produtos.rename(
        columns={
            "id": "produto_id",
            "nome": "produto_nome",
            "categoria": "produto_categoria",
            "preco": "produto_preco",
            "criado_em": "produto_criado_em",
        }
    )

    if "produto_criado_em" in df.columns:
        df["produto_criado_em"] = pd.to_datetime(df["produto_criado_em"])

    # Garante tipo numérico no preço
    df["produto_preco"] = pd.to_numeric(df["produto_preco"], errors="coerce")

    return df

def build_vendas_enriquecidas(
    vendas: pd.DataFrame,
    clientes: pd.DataFrame,
    produtos: pd.DataFrame,
) -> pd.DataFrame:
    df = vendas.copy()

    # Normaliza nome de colunas de chave
    df = df.rename(
        columns={
            "id": "venda_id",
            "cliente_id": "cliente_id",
            "produto_id": "produto_id",
            "quantidade": "quantidade",
            "valor_total": "valor_total",
            "data_venda": "data_venda",
        }
    )

    df["data_venda"] = pd.to_datetime(df["data_venda"])
    df["valor_total"] = pd.to_numeric(df["valor_total"], errors="coerce")

    # Join com clientes
    df = df.merge(clientes, on="cliente_id", how="left")

    # Join com produtos
    df = df.merge(produtos, on="produto_id", how="left")

    # Ordena colunas em uma ordem mais analítica
    col_order = [
        "venda_id",
        "data_venda",
        "cliente_id",
        "cliente_nome",
        "cliente_email",
        "produto_id",
        "produto_nome",
        "produto_categoria",
        "quantidade",
        "valor_total",
        "produto_preco",
    ]
    df = df[[c for c in col_order if c in df.columns]]

    return df

def main():
    clientes_bz, produtos_bz, vendas_bz = load_bronze_tables()

    clientes_sl = clean_dim_clientes(clientes_bz)
    produtos_sl = clean_dim_produtos(produtos_bz)
    vendas_enriq = build_vendas_enriquecidas(vendas_bz, clientes_sl, produtos_sl)

    clientes_sl.to_parquet(SILVER_DIR / "clientes.parquet", index=False)
    produtos_sl.to_parquet(SILVER_DIR / "produtos.parquet", index=False)
    vendas_enriq.to_parquet(SILVER_DIR / "vendas_enriquecidas.parquet", index=False)

    print("Camada silver gerada em", SILVER_DIR)

if __name__ == "__main__":
    main()
