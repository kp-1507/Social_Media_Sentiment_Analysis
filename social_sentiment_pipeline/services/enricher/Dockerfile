FROM python:3.10

WORKDIR /app

COPY enricher.py .
COPY healthcheck.py .

RUN pip install azure-eventhub

CMD ["python", "enricher.py"] 