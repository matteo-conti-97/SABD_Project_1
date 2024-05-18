FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

WORKDIR /opt/hadoop-3.2.1/etc/hadoop/

RUN rm ./hdfs-site.xml

COPY ./hdfs-config/hdfs-site.xml ./hdfs-site.xml