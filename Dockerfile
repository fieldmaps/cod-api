FROM ubuntu:24.10

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    gdal-bin python3-poetry \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

COPY poetry.lock ./
COPY poetry.toml ./
COPY pyproject.toml ./
RUN poetry install --no-root --no-dev --no-cache
ENV PATH="/usr/src/app/.venv/bin:$PATH"

COPY inputs/.gitignore ./inputs/.gitignore
COPY outputs/.gitignore ./outputs/.gitignore
COPY app ./app

CMD ["python", "-m", "app"]
