#!/usr/bin/env bash
# Note: This is meant to be run on the machine to perform experiments on.
USAGE_STRING="Usage: volume.sh [shopalot | tpc_ch]"

# Configure the appropriate data path.
if [[ $# -eq 1 ]] && [[ $1 == "shopalot" ]]; then
  DATA_PATH=$(jq -r .dataPath config/shopalot.json)
  echo "Using data path: ${DATA_PATH}"
elif [[ $# -eq 1 ]] && [[ $1 == "tpc_ch" ]]; then
  DATA_PATH=$(jq -r .dataPath config/tpc_ch.json)
  echo "Using data path: ${DATA_PATH}"
else
  echo "$USAGE_STRING"
  exit 1
fi

echo 'Spawning container to copy the volume to.'
docker run --detach \
  --name volumes_ \
  --mount source=$1,destination=/resources \
  ubuntu:18.04

echo 'Copying the data files to the volume.'
docker cp resources/sample.json volumes_:/resources
docker cp ${DATA_PATH} volumes_:/resources

echo 'Removing the temporary container.'
docker rm -f volumes_ || true
