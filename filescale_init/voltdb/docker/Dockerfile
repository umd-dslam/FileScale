# VoltDB on top of Docker base JDK8 images
FROM java:8
WORKDIR /root
COPY voltdb-ent/ voltdb-ent/
COPY deploy.py voltdb-ent/
WORKDIR /root/voltdb-ent
# Ports
# 21212 : Client Port
# 21211 : Admin Port
#  8080 : Web Interface Port
#  3021 : Internal Server Port
#  4560 : Log Port
#  9090 : JMX Port
#  5555 : Replication Port
#  7181 : Zookeeper Port
EXPOSE 21212 21211 8080 3021 4560 9090 5555 7181
CMD /bin/bash
