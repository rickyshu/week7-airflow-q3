FROM apache/airflow:3.4.0

USER root
RUN sed -i 's|http://deb.debian.org/|https://deb.debian.org/|g' /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends -o Acquire::https::Timeout=45 -o Acquire::Retries=2 openjdk-21-jdk-headless \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /opt/airflow/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /opt/airflow/requirements.txt
