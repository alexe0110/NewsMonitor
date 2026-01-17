FROM apache/airflow:3.1.6-python3.13

RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/home/airflow/.cargo/bin:$PATH"

COPY pyproject.toml uv.lock ./

RUN uv sync --frozen --no-dev --no-install-project && uv cache clean

# Activate env (set use by default)
ENV VIRTUAL_ENV=/opt/airflow/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"