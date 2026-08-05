FROM jupyter/scipy-notebook:latest

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

RUN uv pip install --system --no-cache \
    psycopg2-binary \
    sqlalchemy \
    requests