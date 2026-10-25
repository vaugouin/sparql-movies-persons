#!/bin/bash

# Switched off by off.sh, back on with on.sh: the flag file CRON_OFF next to this script makes
# every cron tick exit here. The script keeps its name, so the git clone stays clean.
if [ -f "$(dirname "$(readlink -f "$0")")/CRON_OFF" ]; then
    echo "CRON_OFF present: sparql-movies-persons switched off (bash on.sh to switch it back on)."
    exit 0
fi

# Check if the sparql-movies-persons Docker container is running
if [ $(docker ps -q -f name=sparql-movies-persons) ]; then
    echo "sparql-movies-persons Docker container is already running."
else
    # Start the sparql-movies-persons container if it is not running
    cd /home/debian/docker/sparql-movies-persons
    docker build -t sparql-movies-persons-python-app .
    # Secrets are passed via --env-file at runtime; they are NOT baked into the image (see .dockerignore).
    # The env file lives outside the app source tree to avoid being picked up by any future COPY.
    # docker run -it --rm --network="host" --env-file /home/debian/docker/sparql-movies-persons/.env --name sparql-movies-persons sparql-movies-persons-python-app
    docker run -d --rm --network="host" --env-file /home/debian/docker/sparql-movies-persons/.env --name sparql-movies-persons sparql-movies-persons-python-app
    echo "sparql-movies-persons Docker container started."
fi
