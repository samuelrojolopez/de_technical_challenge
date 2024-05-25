FROM apache/airflow:2.9.1
COPY requirements.txt .
RUN pip install -r requirements.txt