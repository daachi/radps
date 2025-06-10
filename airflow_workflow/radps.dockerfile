FROM apache/airflow:3.0.0

USER airflow

RUN git clone https://github.com/casangi/RADPS.git

COPY RADPS/airflow_workflow/dags/* /opt/airflow/dags/
