FROM python:3.13-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv via official installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

COPY requirements.txt .
RUN /root/.local/bin/uv pip install --system -r requirements.txt

COPY consumer.py .

ENV KAFKA_BOOTSTRAP_SERVERS=tansu:9092

ENTRYPOINT ["python", "consumer.py"]