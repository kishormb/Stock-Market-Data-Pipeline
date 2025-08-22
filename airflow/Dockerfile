# #FROM apache/airflow:2.9.0-python3.10
# FROM apache/airflow:2.9.1-python3.11

# # Install additional Python dependencies (add more if needed)
# COPY requirements.txt /requirements.txt
# RUN pip install --no-cache-dir -r /requirements.txt


FROM apache/airflow:2.9.1-python3.11

USER root
RUN apt-get update && apt-get upgrade -y && apt-get clean

USER airflow

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt
