FROM debian:jessie

RUN apt-get update && apt-get install -y openjdk-7-jre-headless wget
RUN wget -q -O - http://mirror.ox.ac.uk/sites/rsync.apache.org/zookeeper/zookeeper-3.4.9/zookeeper-3.4.9.tar.gz | tar -xzf - -C /opt \
    && mv /opt/zookeeper-3.4.9 /opt/zookeeper \
    && cp /opt/zookeeper/conf/zoo_sample.cfg /opt/zookeeper/conf/zoo.cfg

RUN (echo "#!/bin/bash \n /opt/zookeeper/bin/zkServer.sh start \n sleep 5d" > /opt/zookeeper/bin/runzoo.sh) && chmod 777 /opt/zookeeper/bin/runzoo.sh

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

EXPOSE 2181 2888 3888

WORKDIR /opt/zookeeper


ENTRYPOINT ["/opt/zookeeper/bin/runzoo.sh"]
CMD ["start"]