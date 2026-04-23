CREATE TABLE IF NOT EXISTS loja.cotacoes (
    id SERIAL PRIMARY KEY,
    base TEXT NOT NULL,
    moeda TEXT NOT NULL,
    taxa NUMERIC(18,6) NOT NULL,
    data_referencia TIMESTAMP NOT NULL,
    criado_em TIMESTAMP DEFAULT NOW()
);
