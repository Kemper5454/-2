FROM apache/spark:3.5.3

USER root
RUN pip3 install --no-cache-dir psycopg2-binary==2.9.10 boto3==1.35.36

# Коннекторы кладём в образ заранее, чтобы при каждом старте не качать их из Maven.
ARG MVN=https://repo1.maven.org/maven2
ADD --chmod=644 ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar /opt/spark/jars/
ADD --chmod=644 ${MVN}/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar /opt/spark/jars/

USER spark
