FROM python:3.10

WORKDIR /app

COPY producer.py .
COPY healthcheck.py .


RUN pip install azure-eventhub

CMD ["python", "producer.py"]