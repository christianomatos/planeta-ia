INSERT INTO loja.clientes (nome, email)
VALUES ('Cliente 1', 'cliente1@example.com'),
       ('Cliente 2', 'cliente2@example.com');

INSERT INTO loja.produtos (nome, categoria, preco)
VALUES ('Produto A', 'Categoria 1', 100.00),
       ('Produto B', 'Categoria 2', 150.00);

INSERT INTO loja.vendas (cliente_id, produto_id, quantidade, valor_total)
VALUES (1, 1, 2, 200.00),
       (2, 2, 1, 150.00);
