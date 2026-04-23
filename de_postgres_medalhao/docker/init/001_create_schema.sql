CREATE SCHEMA IF NOT EXISTS loja;

CREATE TABLE IF NOT EXISTS loja.clientes (
    id SERIAL PRIMARY KEY,
    nome TEXT NOT NULL,
    email TEXT,
    criado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS loja.produtos (
    id SERIAL PRIMARY KEY,
    nome TEXT NOT NULL,
    categoria TEXT,
    preco NUMERIC(10,2),
    criado_em TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS loja.vendas (
    id SERIAL PRIMARY KEY,
    cliente_id INT REFERENCES loja.clientes(id),
    produto_id INT REFERENCES loja.produtos(id),
    quantidade INT NOT NULL,
    valor_total NUMERIC(10,2),
    data_venda DATE DEFAULT CURRENT_DATE
);
