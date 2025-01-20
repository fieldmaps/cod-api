FROM ghcr.io/osgeo/gdal:ubuntu-full-3.10.1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    python3-pip python3-venv \
    && rm -rf /var/lib/apt/lists/*

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY api ./api
COPY app ./app
COPY inputs/level_1.json ./inputs/level_1.json
COPY inputs/level_2.json ./inputs/level_2.json
COPY inputs/m49.csv ./inputs/m49.csv
COPY inputs/unterm.csv ./inputs/unterm.csv
COPY outputs/.gitignore ./outputs/.gitignore

CMD ["fastapi", "run", "api"]
