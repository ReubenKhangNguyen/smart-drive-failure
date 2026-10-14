FROM python:3.11-slim-bookworm

WORKDIR /opt/project
RUN pip install --no-cache-dir kafka-python==2.0.2

ENTRYPOINT ["python", "ingestion/kafka_producer.py"]
