.PHONY: up down logs test lint format bronze silver gold pipeline trino status help wait-spark


COMPOSE=docker compose -f docker/docker-compose.yml
SPARK_EXEC=docker exec lh_spark_master /opt/spark/bin/spark-submit


JARS_BASE=/opt/spark/work-dir/jars/delta-spark_2.12-3.2.0.jar,/opt/spark/work-dir/jars/delta-storage-3.2.0.jar,/opt/spark/work-dir/jars/antlr4-runtime-4.9.3.jar,/opt/spark/work-dir/jars/postgresql-42.7.3.jar
JARS_S3A=$(JARS_BASE),/opt/spark/work-dir/jars/hadoop-aws-3.3.4.jar,/opt/spark/work-dir/jars/aws-java-sdk-bundle-1.12.262.jar


CONFS_BASE=--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
CONFS_S3A=--conf spark.hadoop.fs.s3a.endpoint=http://lh_minio:9000 --conf spark.hadoop.fs.s3a.access.key=minioadmin --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 --conf spark.hadoop.fs.s3a.path.style.access=true --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider


up:
	$(COMPOSE) up -d


down:
	$(COMPOSE) down


status:
	$(COMPOSE) ps


logs:
	$(COMPOSE) logs -f


wait-spark:
	until docker exec lh_spark_master true >/dev/null 2>&1; do \
		echo "Aguardando lh_spark_master subir..."; \
		sleep 2; \
	done


test:
	pytest tests/ -v --tb=short


lint:
	flake8 spark/ tests/ --max-line-length=120


format:
	black spark/ tests/
	isort spark/ tests/


bronze: wait-spark
	$(SPARK_EXEC) \
		--master local[*] \
		--driver-memory 1g \
		--jars $(JARS_BASE) \
		$(CONFS_BASE) \
		/opt/spark/work-dir/spark/01_bronze_ingestion.py


silver: wait-spark
	$(SPARK_EXEC) \
		--master local[*] \
		--driver-memory 1g \
		--jars $(JARS_BASE) \
		$(CONFS_BASE) \
		/opt/spark/work-dir/spark/02_silver_transform.py

gold: wait-spark
	$(SPARK_EXEC) \
		--master local[*] \
		--driver-memory 1g \
		--jars $(JARS_BASE) \
		$(CONFS_BASE) \
		/opt/spark/work-dir/spark/03_gold_aggregations.py


pipeline: bronze silver gold
	@echo "Pipeline Bronze -> Silver -> Gold concluido!"


trino:
	@echo "Aguardando Trino ficar pronto..."
	@for i in $$(seq 1 90); do \
		if docker exec lh_trino curl -sf http://localhost:8080/v1/info | grep -q '"starting":false'; \
		then \
			break; \
		fi; \
		echo "Trino ainda inicializando..."; \
		sleep 2; \
	done
	docker exec lh_trino trino --server localhost:8080 --catalog delta --schema gold --execute "SHOW TABLES"


help:
	@echo "make up | down | status | logs | bronze | silver | gold | pipeline | trino | test | lint | format"