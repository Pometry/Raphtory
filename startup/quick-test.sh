#/bin/bash

cp base.env .env
cp base.yml singlenode.yml
echo '        volumes:' >> singlenode.yml
echo '            - '$(pwd)':/opt/docker/volume' >> singlenode.yml

bash submit-query.sh &

echo 'Starting up Raphtory ...'
docker-compose -f singlenode.yml up --force-recreate 

if [ $? -ne 0 ]; then
    echo Whoops! Something went wrong!
fi
