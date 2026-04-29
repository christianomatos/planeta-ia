#!/bin/bash
set -e

run_trino() {
  docker exec lh_trino trino --server http://localhost:8080 --execute "$1" 2>&1
}

echo "============================================================"
echo "PASSO 1: Ver catalogos e schemas atuais"
echo "============================================================"
run_trino "SHOW CATALOGS;"
run_trino "SHOW SCHEMAS FROM delta;"

echo
echo "============================================================"
echo "PASSO 2: Criar schemas bronze, silver e gold sem location"
echo "============================================================"
run_trino "CREATE SCHEMA IF NOT EXISTS delta.bronze;"
run_trino "CREATE SCHEMA IF NOT EXISTS delta.silver;"
run_trino "CREATE SCHEMA IF NOT EXISTS delta.gold;"

echo
echo "Schemas após criação:"
run_trino "SHOW SCHEMAS FROM delta;"

echo
echo "============================================================"
echo "PASSO 3: Registrar tabelas Delta existentes"
echo "============================================================"

if [ -d "data/bronze/customers/_delta_log" ]; then
  echo "Registrando bronze.customers"
  run_trino "CALL delta.system.register_table(schema_name => 'bronze', table_name => 'customers', table_location => '/data/bronze/customers');" || true
else
  echo "Pulando bronze.customers: _delta_log não encontrado"
fi

if [ -d "data/bronze/orders/_delta_log" ]; then
  echo "Registrando bronze.orders"
  run_trino "CALL delta.system.register_table(schema_name => 'bronze', table_name => 'orders', table_location => '/data/bronze/orders');" || true
else
  echo "Pulando bronze.orders: _delta_log não encontrado"
fi

if [ -d "data/gold/country_kpis/_delta_log" ]; then
  echo "Registrando gold.country_kpis"
  run_trino "CALL delta.system.register_table(schema_name => 'gold', table_name => 'country_kpis', table_location => '/data/gold/country_kpis');" || true
else
  echo "Pulando gold.country_kpis: _delta_log não encontrado"
fi

echo
echo "============================================================"
echo "PASSO 4: Validar tabelas registradas"
echo "============================================================"
echo "--- bronze ---"
run_trino "SHOW TABLES FROM delta.bronze;" || true

echo
echo "--- gold ---"
run_trino "SHOW TABLES FROM delta.gold;" || true

echo
echo "--- teste bronze.customers ---"
run_trino "SELECT * FROM delta.bronze.customers LIMIT 5;" || true

echo
echo "--- teste bronze.orders ---"
run_trino "SELECT * FROM delta.bronze.orders LIMIT 5;" || true

echo
echo "--- teste gold.country_kpis ---"
run_trino "SELECT * FROM delta.gold.country_kpis LIMIT 5;" || true
EOF

chmod +x fix_trino_schema_register_final.sh
bash fix_trino_schema_register_final.sh