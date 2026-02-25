FROM python:3.11-slim

RUN pip install --no-cache-dir \
    google-cloud-storage \
    pandas \
    pyarrow

WORKDIR /app

COPY load_yellow_green_taxi_data.py .


CMD ["python", "load_yellow_green_taxi_data.py"]