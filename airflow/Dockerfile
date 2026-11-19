FROM apache/airflow:3.1.7

USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/olist-requirements.txt
RUN pip install --no-cache-dir -r /tmp/olist-requirements.txt
