FROM python:3.9.5-buster

# Couchbase requirements.
RUN apt-get update && apt-get install -y \
    git-all \
    cmake \
    build-essential \
    libssl-dev

# Install the requirements for this repository.
COPY requirements.txt requirements.txt
RUN pip3 install -v -r requirements.txt
