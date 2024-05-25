FROM apache/airflow:2.9.1
COPY requirements.txt .
COPY .env .
RUN pip install -r requirements.txt