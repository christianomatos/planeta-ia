import csv
import random
from datetime import datetime, timedelta

from faker import Faker

fake = Faker("pt_BR")

NUM_RECORDS = 3_000_000
OUTPUT_PATH = "src/de_etl_pipeline_loja/data/raw/vendas.csv"

# catálogo simples de produtos e faixas de preço
PRODUCTS = {
    "arroz_5kg": (18.0, 30.0),
    "feijao_1kg": (6.0, 12.0),
    "acucar_1kg": (4.0, 9.0),
    "oleo_soja_900ml": (6.0, 15.0),
    "cafe_500g": (10.0, 25.0),
    "detergente_500ml": (2.5, 6.0),
    "sabao_po_1kg": (10.0, 25.0),
    "refrigerante_2l": (7.0, 15.0),
    "agua_1_5l": (2.0, 5.0),
    "biscoito_200g": (3.0, 8.0),
}

PAYMENT_METHODS = ["pix", "cartao_credito", "cartao_debito", "dinheiro"]

def random_date(start_date: datetime, end_date: datetime) -> datetime:
    delta = end_date - start_date
    offset_days = random.randint(0, delta.days)
    return start_date + timedelta(days=offset_days)

def generate_vendas():
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "id_pedido",
                "data_pedido",
                "hora_pedido",
                "cliente_id",
                "loja_id",
                "produto",
                "preco_unitario",
                "quantidade",
                "valor_total",
                "metodo_pagamento",
                "cidade",
                "estado",
            ]
        )

        for order_id in range(1, NUM_RECORDS + 1):
            order_date = random_date(start_date, end_date)
            order_time = f"{random.randint(8, 22):02d}:{random.randint(0,59):02d}:00"

            cliente_id = random.randint(1, 50_000)
            loja_id = random.randint(1, 50)

            product_name = random.choice(list(PRODUCTS.keys()))
            min_price, max_price = PRODUCTS[product_name]
            preco = round(random.uniform(min_price, max_price), 2)

            quantidade = random.randint(1, 10)
            valor_total = round(preco * quantidade, 2)

            metodo_pagamento = random.choice(PAYMENT_METHODS)

            city = fake.city()
            state = fake.estado_sigla()

            writer.writerow(
                [
                    order_id,
                    order_date.strftime("%Y-%m-%d"),
                    order_time,
                    cliente_id,
                    loja_id,
                    product_name,
                    preco,
                    quantidade,
                    valor_total,
                    metodo_pagamento,
                    city,
                    state,
                ]
            )

    print(f"Arquivo gerado com sucesso em: {OUTPUT_PATH}")

if __name__ == "__main__":
    generate_vendas()