# 1. Pradedame nuo švaraus ir stabilaus Python atvaizdo
FROM python:3.12-slim

# Aplinkos kintamieji, apsaugantys nuo .pyc failų rašymo ir užtikrinantys greitus logus
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# 2. Įdiegiame Java (OpenJDK 21), curl ir procps (procesų kontrolei)
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-21-jre-headless \
    curl \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 3. Dinaminis JAVA_HOME nustatymas
RUN ln -s /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/default-java
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# 5. Nustatome darbinį aplanką
WORKDIR /app

# 6. Diegiame priklausomybes iš requirements.txt
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

# 7. Sukuriame duomenų struktūrą konteinerio viduje
RUN mkdir -p /app/src \
             /app/data/Input_Data \
             /app/data/Interim_Files \
             /app/data/Reports \
             /app/data/Master_Lake && \
    chmod -R 777 /app/data && \
    chmod -R 777 /app

# 8. Spark laikinų failų aplankas
ENV SPARK_LOCAL_DIRS=/tmp/spark-local
RUN mkdir -p /tmp/spark-local && chmod -R 777 /tmp/spark-local

# 9. Nukopijuojame pagrindinį valdymo skriptą
COPY src/pipeline_runner.py .

# Paleidimo komanda
CMD ["python3", "pipeline_runner.py"]

