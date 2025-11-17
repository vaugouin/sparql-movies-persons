#!/bin/bash

# Check if the sparql-movies-persons Docker container is running
if [ $(docker ps -q -f name=sparql-movies-persons) ]; then
    echo "sparql-movies-persons Docker container is already running."
else
    # Start the sparql-movies-persons container if it is not running
    cd /home/debian/docker/sparql-movies-persons
    docker build -t sparql-movies-persons-python-app .
    # docker run -it --rm --network="host" --name sparql-movies-persons sparql-movies-persons-python-app
    docker run -d --rm --network="host" --name sparql-movies-persons sparql-movies-persons-python-app
    echo "sparql-movies-persons Docker container started."
fi
