FROM apache/hive:4.0.0
USER root
RUN apt-get update -qq && \
    apt-get install -y -qq wget && \
    wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
         -O /opt/hive/lib/postgresql-42.7.3.jar && \
    chmod 644 /opt/hive/lib/postgresql-42.7.3.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    echo "Driver JDBC instalado com sucesso"
USER hive
