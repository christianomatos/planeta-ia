.PHONY: up down logs test lint format bronze silver gold pipeline trino status help

up:
	docker compose -f docker/docker-compose.yml up -d
	@echo ""
	@echo "Servicos disponiveis:"
	@echo "  Spark UI   -> http://localhost:8081"
	@echo "  MinIO      -> http://localhost:9001  (minioadmin / minioadmin123)"
	@echo "  Trino      -> http://localhost:8085"
	@echo "  Jupyter    -> http://localhost:8888"

down:
	docker compose -f docker/docker-compose.yml down

status:
	docker compose -f docker/docker-compose.yml ps

logs:
	docker compose -f docker/docker-compose.yml logs -f

test:
	pytest tests/ -v --tb=short

lint:
	flake8 spark/ tests/ --max-line-length=120

format:
	black spark/ tests/
	isort spark/ tests/

bronze:
	docker exec lh_spark_master spark-submit \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/bitnami/spark/work/spark/01_bronze_ingestion.py

silver:
	docker exec lh_spark_master spark-submit \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/bitnami/spark/work/spark/02_silver_transform.py

gold:
	docker exec lh_spark_master spark-submit \
		--packages io.delta:delta-spark_2.12:3.1.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/bitnami/spark/work/spark/03_gold_aggregations.py

pipeline: bronze silver gold
	@echo ""
	@echo "Pipeline Bronze -> Silver -> Gold concluido!"

trino:
	docker exec -it lh_trino trino \
		--server localhost:8080 \
		--catalog delta \
		--schema gold

help:
	@echo ""
	@echo "Comandos disponiveis:"
	@echo "  make up        - Sobe todos os 8 servicos"
	@echo "  make down      - Para todos os servicos"
	@echo "  make status    - Lista status dos containers"
	@echo "  make logs      - Acompanha logs em tempo real"
	@echo "  make pipeline  - Executa Bronze -> Silver -> Gold"
	@echo "  make bronze    - Apenas ingestao Postgres"
	@echo "  make silver    - Apenas transformacao + ZORDER"
	@echo "  make gold      - Apenas agregacoes + Time Travel"
	@echo "  make trino     - Abre shell SQL no Trino"
	@echo "  make test      - Roda testes unitarios"
	@echo "  make lint      - Verifica qualidade do codigo"
	@echo "  make format    - Formata codigo com Black + isort"
	@echo ""
