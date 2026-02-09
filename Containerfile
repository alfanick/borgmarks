FROM python:3.12-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY borgmarks /app/borgmarks
COPY README.md /app/README.md
COPY sample_config.yaml /app/sample_config.yaml

ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "borgmarks"]
