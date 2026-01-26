FROM python:3.13.11-slim

RUN pip install pandas pyarrow

WORKDIR /code
COPY collect-data.py .
COPY collect-data2.py .
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/
COPY pyproject.toml .python-version uv.lock ./
ENV PATH="/code/.venv/bin:$PATH"
RUN uv sync --locked