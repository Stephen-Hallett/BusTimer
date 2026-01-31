# hadolint ignore=DL3007
FROM python:3.13-slim-bookworm

WORKDIR /

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY backend/app app
COPY backend/uv.lock .
COPY backend/pyproject.toml .

RUN uv sync --frozen

CMD ["uv", "run", "fastapi", "run","app", "--port", "8000"]