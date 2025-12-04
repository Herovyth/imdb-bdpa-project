FROM python:3.9-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        curl wget gnupg ca-certificates && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN wget -O - https://packages.adoptium.net/artifactory/api/gpg/key/public \
        | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg && \
    echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] \
        https://packages.adoptium.net/artifactory/deb bullseye main" \
        > /etc/apt/sources.list.d/adoptium.list

RUN apt-get update && apt-get install -y temurin-17-jdk && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyspark numpy

WORKDIR /app
COPY . /app

CMD ["python", "main.py"]
