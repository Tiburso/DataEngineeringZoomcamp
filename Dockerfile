FROM bitnami/spark:3.5.1

COPY spark-jars/*.jar $SPARK_HOME/jars

RUN mkdir -p $SPARK_HOME/secrets
COPY ./infra/creds.json $SPARK_HOME/secrets/gcp-credentials.json
ENV GOOGLE_APPLICATION_CREDENTIALS=$SPARK_HOME/secrets/gcp-credentials.json

RUN pip install delta-spark