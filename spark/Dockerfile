# Image Spark 3.5.3 + Delta Lake 3.2.1 + connecteurs S3 (MinIO) et Kafka.
# Toutes les dépendances sont téléchargées AU BUILD : la démo fonctionne ensuite hors ligne.
# Variante Java 17 : basée sur Ubuntu 22.04 (Python 3.10). La variante « 3.5.3-python3 » est basée sur
# Ubuntu 20.04 (Python 3.8), trop ancienne pour les bibliothèques Python du kit.
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
RUN python3 -c "import sys; assert sys.version_info >= (3, 10), 'Python >= 3.10 requis, trouvé ' + sys.version" \
 && python3 --version

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/*

ARG MVN=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars \
 && curl -fsSLO ${MVN}/io/delta/delta-spark_2.12/3.2.1/delta-spark_2.12-3.2.1.jar \
 && curl -fsSLO ${MVN}/io/delta/delta-storage/3.2.1/delta-storage-3.2.1.jar \
 && curl -fsSLO ${MVN}/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && curl -fsSLO ${MVN}/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
 && curl -fsSLO ${MVN}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO ${MVN}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
 && curl -fsSLO ${MVN}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
 && curl -fsSLO ${MVN}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar

# Python >= 3.10 : les versions récentes s'installent telles quelles. (Sur la
# variante Ubuntu 20.04 / Python 3.8, il fallait rétrograder numpy, pandas et
# scikit-learn, et épingler urllib3 < 1.27 pour botocore — plus nécessaire ici.)
RUN pip install --no-cache-dir \
      numpy==1.26.4 pandas==2.2.3 pyarrow==17.0.0 scikit-learn==1.5.2 \
      mlflow==2.16.2 kafka-python==2.0.2 boto3==1.35.36

COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY log4j2.properties /opt/spark/conf/log4j2.properties

# PySpark est fourni par Spark lui-même : on le rend importable par « python3 ».
ENV PATH="${PATH}:/opt/spark/bin" \
    PYSPARK_PYTHON=python3 \
    SPARK_HOME=/opt/spark \
    PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
RUN test -f /opt/spark/python/lib/py4j-0.10.9.7-src.zip \
 && python3 -c "import pyspark; print('PySpark', pyspark.__version__)"

WORKDIR /workspace
