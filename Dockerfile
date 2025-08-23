FROM apache/airflow:2.10.5-python3.9

USER root

# Optionally install OS-level dependencies here if needed:
# RUN apt-get update && apt-get install -y <some-package>

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    clickhouse-connect \
    apache-airflow-providers-apache-kafka \
    apache-airflow-providers-postgres
