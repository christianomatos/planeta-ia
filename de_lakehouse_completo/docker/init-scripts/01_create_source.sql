-- Cria banco separado para o Hive Metastore
CREATE DATABASE hive_metastore;

\c source_db;

CREATE TABLE IF NOT EXISTS customers (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE,
    country     VARCHAR(50),
    created_at  TIMESTAMP DEFAULT NOW(),
    updated_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    customer_id  INT REFERENCES customers(id),
    product      VARCHAR(100),
    amount       NUMERIC(10, 2),
    status       VARCHAR(20) DEFAULT 'pending',
    order_date   DATE DEFAULT CURRENT_DATE,
    updated_at   TIMESTAMP DEFAULT NOW()
);

INSERT INTO customers (name, email, country) VALUES
  ('Alice Silva',    'alice@demo.com',    'Brazil'),
  ('Bob Santos',     'bob@demo.com',      'Brazil'),
  ('Carol Oliveira', 'carol@demo.com',    'Argentina'),
  ('Dave Lima',      'dave@demo.com',     'Brazil');

INSERT INTO orders (customer_id, product, amount, status) VALUES
  (1, 'Laptop',      4500.00, 'completed'),
  (1, 'Mouse',         89.90, 'completed'),
  (2, 'Monitor',     1200.00, 'pending'),
  (3, 'Keyboard',     350.00, 'completed'),
  (4, 'Headphones',   299.90, 'cancelled');
