# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/devcontainers/python:1-3.11-bullseye

# Install database client libraries needed by psycopg2 and SQLite tooling.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq-dev sqlite3 \
    && rm -rf /var/lib/apt/lists/*
