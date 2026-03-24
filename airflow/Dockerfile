FROM apache/airflow:3.0.0

USER root
RUN pip install --no-cache-dir -r /requirements.txt
USER airflow

ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
