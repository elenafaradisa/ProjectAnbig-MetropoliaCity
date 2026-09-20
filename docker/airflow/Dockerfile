FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# JDBC driver so Spark can write to PostgreSQL
RUN mkdir -p /opt/jars \
    && curl -fsSL -o /opt/jars/postgresql-42.7.3.jar \
       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV JDBC_JAR=/opt/jars/postgresql-42.7.3.jar

USER airflow
RUN pip install --no-cache-dir pyspark==3.5.1 psycopg2-binary==2.9.9 pyyaml==6.0.1
