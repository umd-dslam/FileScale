# VoltDB on top of Docker base JDK8 images
FROM java:8
MAINTAINER Gang Liao <gangliao@cs.umd.edu>

# Export the VOLTDB_VERSION, VOLTDB_DIR and binaries to the PATH
ENV VOLTDB_VERSION 8.4.2
ENV VOLTDB_DIR /usr/local/opt/voltdb
ENV PATH $PATH:$VOLTDB_DIR/$VOLTDB_VERSION/bin

# Build and cleanup everything after compilation
WORKDIR /tmp

RUN echo "deb [check-valid-until=no] http://cdn-fastly.deb.debian.org/debian jessie main" > /etc/apt/sources.list.d/jessie.list
RUN echo "deb [check-valid-until=no] http://archive.debian.org/debian jessie-backports main" > /etc/apt/sources.list.d/jessie-backports.list
RUN sed -i '/deb http:\/\/deb.debian.org\/debian jessie-updates main/d' /etc/apt/sources.list
RUN apt-get -o Acquire::Check-Valid-Until=false update

RUN set -xe \
  && buildDeps=' \
      ant \
      build-essential \
      curl \
      ccache \
      cmake \
  ' \
  && apt-get install -y --no-install-recommends $buildDeps \
  && rm -rf /var/lib/apt/lists/* \
  && curl -fSL https://github.com/VoltDB/voltdb/archive/voltdb-${VOLTDB_VERSION}.tar.gz | tar zx

RUN cd /tmp/voltdb-voltdb-${VOLTDB_VERSION} \
  && ant -Djmemcheck=NO_MEMCHECK

RUN mkdir -p ${VOLTDB_DIR}/${VOLTDB_VERSION} \
  && cd ${VOLTDB_DIR}/${VOLTDB_VERSION}

RUN for file in LICENSE README.md README.thirdparty bin bundles doc examples lib third_party/python tools version.txt voltdb; do \
      cp -R /tmp/voltdb-voltdb-${VOLTDB_VERSION}/${file} .; done

RUN mkdir -p third_party \
  && mv python third_party \
  && apt-get purge -y --auto-remove $buildDeps \
  && rm -rf /tmp/voltdb-voltdb-${VOLTDB_VERSION}

# Our default VoltDB work dir
WORKDIR /usr/local/var/voltdb
COPY deploy.py voltdb-ent/

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
