FROM python:3.10-slim

WORKDIR /app

# Instala as dependências necessárias
RUN pip install --no-cache-dir faust-streaming neo4j drain3 opentelemetry-proto river


# Copia os códigos do ecossistema do framework para dentro do container
COPY database.py .
COPY log_consumer.py .
COPY trace_consumer.py .
COPY drain3.ini .
COPY anomaly_streaming_detector.py .
COPY candidate_cause_agent.py .
COPY causal_inference_agent.py .

# O comando padrão é omitido aqui porque foi customizado direto no docker-compose via "command"

