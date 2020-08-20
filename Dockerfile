########
# Base #
########
FROM ubuntu:20.10 AS base

ENV SPARK_MASTER local[*]
ENV DEPLOY_MODE client
ENV HADOOP_VERSION 3.2.1
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH ${HADOOP_HOME}/bin:$PATH
ENV AWS_ACCESS_KEY_ID TESTING
ENV AWS_SECRET_ACCESS_KEY TESTING
ENV S3_ENDPOINT http://s3:2000

RUN apt-get update -y && apt-get install -y openjdk-8-jre-headless && apt-get clean

########################
# Fetch and build deps #
########################
FROM base AS build

RUN apt-get update -y && apt-get install -y \
    openjdk-8-jdk \
    wget

# Apache Hadoop
RUN wget -q -O /tmp/hadoop.tgz http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
RUN tar -xzf /tmp/hadoop.tgz -C /opt/
RUN mv /opt/hadoop-$HADOOP_VERSION /opt/hadoop
RUN rm -rf /opt/hadoop/share/doc
RUN mv /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar /opt/hadoop/share/hadoop/common/aws-java-sdk-bundle-1.11.375.jar
RUN mv /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-$HADOOP_VERSION.jar /opt/hadoop/share/hadoop/common/hadoop-aws-$HADOOP_VERSION.jar
RUN rm -r /opt/hadoop/share/hadoop/tools
# Compile WordCount.java for Mapreduce test
COPY test/fixtures/WordCount.java /opt/WordCount.java
RUN mkdir /opt/wordcount_classes
RUN javac -classpath $(hadoop classpath) -d /opt/wordcount_classes /opt/WordCount.java
RUN jar -cvf /opt/wc-mapreduce.jar -C /opt/wordcount_classes/ .
# Compile NonChunkedDefaultS3ClientFactory.java for local S3
COPY test/fixtures/NonChunkedDefaultS3ClientFactory.java /opt/NonChunkedDefaultS3ClientFactory.java
RUN mkdir /opt/s3_client_factory_classes
RUN javac -classpath $(hadoop classpath) -d /opt/s3_client_factory_classes /opt/NonChunkedDefaultS3ClientFactory.java
RUN jar -cvf /opt/non-chunked-default-s3-clientfactory.jar -C /opt/s3_client_factory_classes/ .


######################
# Fetch runtime deps #
######################
FROM base AS app

COPY requirements.txt /requirements.txt

RUN apt-get update -y && apt-get install -y \
    python3.7 \
    python3-pip \
  && apt-get clean \
  && pip install --no-cache-dir -r requirements.txt \
  && mkdir /opt/localemr \
  && mkdir /opt/hadoop

WORKDIR /opt/localemr

COPY --from=build /opt/hadoop /opt/hadoop/
COPY --from=build /opt/non-chunked-default-s3-clientfactory.jar /opt/hadoop/share/hadoop/common/
COPY conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY --from=build /opt/wc-mapreduce.jar /opt/hadoop/wc-mapreduce.jar

COPY . .

CMD ["./entrypoint.sh"]

###################
# Fetch test deps #
####################
FROM app AS test

COPY requirements-dev.txt .

RUN pip install --no-cache-dir -r requirements-dev.txt
