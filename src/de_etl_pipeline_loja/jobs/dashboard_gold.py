from pathlib import Path

from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import plotly.io as pio
import pandas as pd

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Tema escuro padrão do Plotly
pio.templates.default = "plotly_dark"

# Diretórios base
THIS_DIR = Path(__file__).resolve().parent          # .../src/de_etl_pipeline_loja/jobs
PACKAGE_ROOT = THIS_DIR.parent                      # .../src/de_etl_pipeline_loja

# Caminho absoluto da camada Gold (Delta)
GOLD_PATH = PACKAGE_ROOT / "data" / "gold" / "vendas_produto_mensal"


def get_spark():
    builder = (
        SparkSession.builder
        .appName("dashboard_gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def load_gold_df() -> pd.DataFrame:
    """
    Lê a tabela Gold em Delta e devolve um DataFrame Pandas.
    Usa caminho absoluto, independente de onde o script é executado.
    """
    spark = get_spark()
    df_spark = spark.read.format("delta").load(str(GOLD_PATH))
    pdf = df_spark.toPandas()
    spark.stop()
    return pdf


# Carrega dados da Gold
df = load_gold_df()

# Garante tipos simples para filtros
df["ano"] = df["ano"].astype(int)
df["mes"] = df["mes"].astype(int)

anos = sorted(df["ano"].unique())
meses = sorted(df["mes"].unique())

# Cria app Dash
app = Dash(__name__)
app.title = "Painel Vendas - Camada Gold"

# Layout com tema escuro, filtros, cards e gráficos
app.layout = html.Div(
    style={
        "backgroundColor": "#111827",
        "color": "#f9fafb",
        "minHeight": "100vh",
        "padding": "10px",
    },
    children=[
        html.H1("Painel de Vendas (Camada Gold)"),

        html.Div([
            # Filtros
            html.Div([
                html.Label("Ano"),
                dcc.Dropdown(
                    id="filtro-ano",
                    options=[{"label": str(a), "value": a} for a in anos],
                    value=anos[-1],
                    clearable=False,
                ),
                html.Br(),
                html.Label("Mês"),
                dcc.Dropdown(
                    id="filtro-mes",
                    options=[{"label": str(m), "value": m} for m in meses],
                    value=meses[-1],
                    clearable=False,
                ),
            ], style={
                "width": "20%",
                "display": "inline-block",
                "verticalAlign": "top",
                "padding": "10px",
                "backgroundColor": "#1f2937",
                "borderRadius": "8px",
            }),

            # Cards
            html.Div([
                html.Div(id="card-receita", style={"margin": "10px", "fontSize": 18}),
                html.Div(id="card-quantidade", style={"margin": "10px", "fontSize": 18}),
            ], style={
                "width": "75%",
                "display": "inline-block",
                "padding": "10px",
            }),
        ]),

        # Gráficos
        html.Div([
            dcc.Graph(id="grafico-receita-produto"),
            dcc.Graph(id="grafico-quantidade-produto"),
            dcc.Graph(id="grafico-receita-mensal"),
        ]),
    ]
)


@app.callback(
    [
        Output("grafico-receita-produto", "figure"),
        Output("grafico-quantidade-produto", "figure"),
        Output("grafico-receita-mensal", "figure"),
        Output("card-receita", "children"),
        Output("card-quantidade", "children"),
    ],
    [
        Input("filtro-ano", "value"),
        Input("filtro-mes", "value"),
    ],
)
def atualizar_painel(ano, mes):
    # 1) Filtra pelo ano e mês selecionados para os gráficos de barra
    filtrado = df[(df["ano"] == ano) & (df["mes"] == mes)]

    # Ordena produtos por receita para o primeiro gráfico
    filtrado_receita = filtrado.sort_values("receita_total", ascending=False)

    # Gráfico 1: Receita por produto
    fig_receita = px.bar(
        filtrado_receita,
        x="produto",
        y="receita_total",
        title=f"Receita por Produto - {mes}/{ano}",
        color_discrete_sequence=["#60a5fa"],
    )

    # Ordena produtos por quantidade para o segundo gráfico
    filtrado_qtd = filtrado.sort_values("quantidade_total", ascending=False)

    # Gráfico 2: Quantidade por produto
    fig_quantidade = px.bar(
        filtrado_qtd,
        x="produto",
        y="quantidade_total",
        title=f"Quantidade por Produto - {mes}/{ano}",
        color_discrete_sequence=["#f97316"],
    )

    # 2) Gráfico de linha: evolução mensal da receita no ano selecionado
    df_ano = (
        df[df["ano"] == ano]
        .groupby("mes", as_index=False)["receita_total"].sum()
        .sort_values("mes")
    )

    fig_linha = px.line(
        df_ano,
        x="mes",
        y="receita_total",
        markers=True,
        title=f"Evolução Mensal da Receita - {ano}",
    )

    # 3) Cards
    receita_total = float(filtrado["receita_total"].sum())
    quantidade_total = int(filtrado["quantidade_total"].sum())

    card_receita = f"Receita total no período: R$ {receita_total:,.2f}"
    card_quantidade = f"Quantidade total de itens vendidos: {quantidade_total:,}"

    return fig_receita, fig_quantidade, fig_linha, card_receita, card_quantidade


if __name__ == "__main__":
    app.run(debug=True)