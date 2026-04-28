FROM apache/airflow:2.9.3

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_LOCAL_IP=127.0.0.1

# voltar para o usuário airflow antes de usar pip
USER airflow

RUN pip install --no-cache-dir pyspark pyarrow