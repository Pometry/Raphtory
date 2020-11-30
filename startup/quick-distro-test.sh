#/bin/bash

cp base.env .env

echo 'Starting up Raphtory ...'
docker-compose -f cluster.yml up --force-recreate 

if [ $? -ne 0 ]; then
    echo Whoops! Something went wrong!
fi
