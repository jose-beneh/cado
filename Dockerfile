# ============================================================================
# ESTÁGIO 1: BUILDER (Compilação isolada do River com Rust)
# ============================================================================
FROM python:3.10-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    librdkafka-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# CORREÇÃO: URL direta do instalador Unix para evitar redirecionamento de tela
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build
RUN pip install --upgrade pip setuptools wheel

RUN pip wheel --no-cache-dir --wheel-dir=/build/wheels \
    river \
    confluent-kafka \
    neo4j \
    opentelemetry-proto \
    requests \
    numpy

# ============================================================================
# ESTÁGIO 2: RUNTIME (Imagem final padronizada e autossuficiente)
# ============================================================================
FROM python:3.10-slim


RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka1 \
    && rm -rf /var/lib/apt/lists/*


WORKDIR /app


# Instala as dependências pré-compiladas trazidas do builder
COPY --from=builder /build/wheels /wheels
RUN pip install --no-index --find-links=/wheels /wheels/* && rm -rf /wheels
RUN pip install --no-cache-dir confluent-kafka neo4j drain3 opentelemetry-proto


# Incorporação estática dos scripts conforme padrão de design
COPY hst_multimodal_detector.py .
COPY database.py .
COPY log_consumer.py .
COPY drain3.ini .
COPY trace_consumer.py .


ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

