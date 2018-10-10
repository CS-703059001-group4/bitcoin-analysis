FROM python:2.7

RUN apt-get update \
    && apt-get install -y openjdk-8-jdk \
    && apt-get install -y ant \
    && apt-get clean

RUN apt-get update \
    && apt-get install ca-certificates-java \
    && apt-get clean \
    && update-ca-certificates -f

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/

RUN pip install --upgrade pip
RUN pip install ipython

ENV HADOOP_VERSION 3.0.1
ENV HADOOP_HOME /usr/hadoop-$HADOOP_VERSION
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH $PATH:$HADOOP_HOME/bin
RUN curl -sL --retry 3 \
    "http://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" \
    | gunzip \
    | tar -x -C /usr/ && \
    rm -rf $HADOOP_HOME/share/doc

ENV SPARK_VERSION 2.3.1
ENV SPARK_PACKAGE spark-${SPARK_VERSION}-bin-without-hadoop
ENV SPARK_HOME /usr/spark-${SPARK_VERSION}
ENV SPARK_DIST_CLASSPATH="$HADOOP_HOME/etc/hadoop/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/hdfs/lib/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/tools/lib/*"
ENV PATH $PATH:${SPARK_HOME}/bin

RUN curl -sL --retry 3 \
    "https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=spark/spark-${SPARK_VERSION}/${SPARK_PACKAGE}.tgz" \
    | gunzip \
    | tar x -C /usr/ \
    && mv /usr/$SPARK_PACKAGE $SPARK_HOME \
    && chown -R root:root $SPARK_HOME
RUN pip install pyspark
