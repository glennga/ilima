#!/usr/bin/env bash
# Note: This is meant to be run on the machine to perform experiments on.
USAGE_STRING="Usage: container.sh [asterixdb | couchbase | mongodb | mysql] [shopalot | tpc_ch]"

# Experiment specific settings.
if [[ $# -eq 2 ]] && [[ $2 == "shopalot" ]]; then
  DATA_PATH=$(jq -r .dataPath config/shopalot.json)
  echo "Using data path: ${DATA_PATH}"
elif [[ $# -eq 2 ]] && [[ $2 == "tpc_ch" ]]; then
  DATA_PATH=$(jq -r .dataPath config/tpc_ch.json)
  echo "Using data path: ${DATA_PATH}"
else
  echo "$USAGE_STRING"
  exit 1
fi

# Platform specific settings.
if [[ $1 == "asterixdb" ]]; then
  echo "Launching instance of AsterixDB."
  PACKAGE_PATH=$(jq -r .package config/asterixdb.json)
  echo "Copying package from ${PACKAGE_PATH}."
  docker rm -f asterixdb_ || true
  docker pull ubuntu
  echo -e "
    FROM ubuntu:18.04
    RUN mkdir /resources
    RUN apt-get update && apt install -y \
      openjdk-11-jdk \
      supervisor
    COPY ${PACKAGE_PATH} /asterixdb
    COPY ${DATA_PATH} /${DATA_PATH}
    COPY resources/sample.json /resources/sample.json
    COPY tools/asterixdb/supervisord.conf /etc/supervisor/conf.d/supervisord.conf
    COPY tools/asterixdb/cc.conf /asterixdb/cc-master.conf
    CMD /usr/bin/supervisord
  " | docker build -t ilima/asterixdb -f- .
  docker run --detach \
    --name asterixdb_ \
    --network="host" \
    ilima/asterixdb

elif [[ $1 == "couchbase" ]]; then
  echo "Launching instance of Couchbase."
  docker pull couchbase
  docker rm -f couchbase_ || true
  echo -e "
    FROM couchbase
    COPY ${DATA_PATH} /${DATA_PATH}
  " | docker build -t ilima/couchbase -f- .
  docker run --detach \
    --name couchbase_ \
    --network="host" \
    ilima/couchbase

elif [[ $1 == "mongodb" ]]; then
  echo "Launching instance of MongoDB."
  docker pull mongodb
  docker rm -f mongodb_ || true
  echo -e "
    FROM mongodb
    COPY ${DATA_PATH} /${DATA_PATH}
  " | docker build -t ilima/mongodb -f- .
  docker run --detach \
    --name mongodb_ \
    --network="host" \
    ilima/mongodb

elif [[ $1 == "mysql" ]]; then
  echo "Launching instance of MySQL."
  docker pull mysql
  docker rm -f mysql_ || true
  echo -e "
    FROM mysql
    ENV MYSQL_ROOT_PASSWORD=AutumnNeverFalls5
    COPY ${DATA_PATH} /${DATA_PATH}
  " | docker build -t ilima/mysql -f- .
  docker run --detach \
    --name mysql_ \
    --network="host" \
    ilima/mysql

else
  echo "$USAGE_STRING"
  exit 1
fi
