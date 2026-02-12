FROM python:3.11-slim

RUN pip install --no-cache-dir google-cloud-storage

WORKDIR /app

COPY load_yellow_taxi_data.py .


CMD ["python", "load_yellow_taxi_data.py"]