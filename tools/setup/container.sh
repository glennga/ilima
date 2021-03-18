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
    COPY resources/sample.json /resources/sample.json
  " | docker build -t ilima/couchbase -f- .
  docker run --detach \
    --name couchbase_ \
    --network="host" \
    ilima/couchbase
  echo "Waiting for container to spin up..."
  sleep 10
  docker exec couchbase_ /opt/couchbase/bin/couchbase-cli cluster-init \
    --cluster-username "$(jq -r .username config/couchbase.json)" \
    --cluster-password "$(jq -r .password config/couchbase.json)" \
    --services data,index,query,fts \
    --cluster-ramsize "$(jq -r .cluster.ramsize config/couchbase.json)" \
    --cluster-index-ramsize "$(jq -r .cluster.indexRamsize config/couchbase.json)" \
    --cluster-fts-ramsize "$(jq -r .cluster.ftsRamsize config/couchbase.json)" \
    --index-storage-setting default
  sleep 1
  docker exec couchbase_ /opt/couchbase/bin/couchbase-cli bucket-create \
    --cluster localhost \
    --username "$(jq -r .username config/couchbase.json)" \
    --password "$(jq -r .password config/couchbase.json)" \
    --bucket "$(jq -r .cluster.bucket config/couchbase.json)" \
    --bucket-type couchbase \
    --bucket-ramsize "$(jq -r .cluster.ramsize config/couchbase.json)"

elif [[ $1 == "mongodb" ]]; then
  echo "Launching instance of MongoDB."
  docker pull mongo
  docker rm -f mongodb_ || true
  echo -e "
    FROM mongo
    ENV MONGO_INITDB_ROOT_USERNAME=$(jq -r .username config/mongodb.json)
    ENV MONGO_INITDB_ROOT_PASSWORD=$(jq -r .password config/mongodb.json)
    COPY ${DATA_PATH} /${DATA_PATH}
    COPY resources/sample.json /resources/sample.json
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
    ENV MYSQL_ROOT_PASSWORD=$(jq -r .password config/mysql.json)
    ENV MYSQL_USER=$(jq -r .username config/mysql.json)
    ENV MYSQL_PASSWORD=$(jq -r .password config/mysql.json)
    ENV MYSQL_DATABASE=$(jq -r .database config/mysql.json)
    COPY ${DATA_PATH} /${DATA_PATH}
    COPY resources/sample.json /resources/sample.json
  " | docker build -t ilima/mysql -f- .
  docker run --detach \
    --name mysql_ \
    --network="host" \
    ilima/mysql
  echo "Waiting for container to spin up..."
  sleep 10
  docker exec mysql_ mysql \
    --user "root" \
    --password="$(jq -r .password config/couchbase.json)" \
    --execute "
      GRANT ALL PRIVILEGES ON *.*
      TO '$(jq -r .username config/mysql.json)'@'%'
      WITH GRANT OPTION;
      FLUSH PRIVILEGES;
    "

else
  echo "$USAGE_STRING"
  exit 1
fi
