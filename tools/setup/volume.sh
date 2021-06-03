#!/usr/bin/env bash
# Note: This is meant to be run on the machine to perform experiments on.
USAGE_STRING="Usage: volume.sh [shopalot | tpc_ch] [volume name]?"

# Configure the appropriate data path.
if (( 1 <= $# <= 2)) && [[ $1 == "shopalot" ]]; then
  DATA_PATH=$(jq -r .dataPath config/shopalot.json)
  echo "Using data path: ${DATA_PATH}"
elif (( 1 <= $# <= 2)) && [[ $1 == "tpc_ch" ]]; then
  DATA_PATH=$(jq -r .dataPath config/tpc_ch.json)
  echo "Using data path: ${DATA_PATH}"
else
  echo "$USAGE_STRING"
  exit 1
fi

# If specified, use a custom name for the volume.
if [[ $# -eq 2 ]]; then
  VOLUME_NAME=$2
else
  VOLUME_NAME=$1
fi

echo 'Spawning container to copy the volume to.'
docker run --detach \
  --name volumes_ \
  --mount source="$VOLUME_NAME",destination=/resources \
  ubuntu:18.04

echo 'Copying the data files to the volume.'
docker cp resources/sample.json volumes_:/resources
docker cp ${DATA_PATH} volumes_:/resources

echo 'Removing the temporary container.'
docker rm -f volumes_ || true
