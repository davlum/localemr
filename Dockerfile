FROM ubuntu:20.10

ENV SPARK_MASTER local[*]
ENV DEPLOY_MODE client
ENV HADOOP_VERSION 3.2.1
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64
ENV HADOOP_HOME /opt/hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop
ENV PATH ${HADOOP_HOME}/bin:$PATH

COPY requirements.txt /requirements.txt

RUN apt-get update -y && apt-get install -y \
    openjdk-8-jre-headless \
    python3.7 \
    wget \
    python3-pip \
  && apt-get clean \
  && pip install --no-cache-dir -r requirements.txt \
  # Apache Hadoop
  && wget -q -O /tmp/hadoop.tgz http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz \
  && tar -xzf /tmp/hadoop.tgz -C /opt/ \
  && mv /opt/hadoop-$HADOOP_VERSION /opt/hadoop \
  && rm -rf /opt/hadoop/share/doc \
  && mkdir -p /tmp/spark-events \
  && mv /opt/hadoop/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar /opt/hadoop/share/hadoop/common/aws-java-sdk-bundle-1.11.375.jar \
  && mv /opt/hadoop/share/hadoop/tools/lib/hadoop-aws-$HADOOP_VERSION.jar /opt/hadoop/share/hadoop/common/hadoop-aws-$HADOOP_VERSION.jar \
  && rm -r /opt/hadoop/share/hadoop/tools \
  && rm -r /tmp/* \
  && mkdir -p /tmp/spark-events \
  && mkdir /opt/localemr

COPY conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY conf/s3_mocktest_demo_2.11-0.0.1.jar /opt/hadoop/share/hadoop/common/s3_mocktest_demo_2.11-0.0.1.jar

WORKDIR /opt/localemr

COPY . .

CMD ["python3", "main.py"]

