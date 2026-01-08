FROM python:3.12

WORKDIR /app

COPY pyproject.toml ./
COPY uv.lock ./
RUN apt-get update && apt-get install -y git gcc libpq-dev
RUN pip install uv && uv sync

ENV PYTHONPATH=/app/src

