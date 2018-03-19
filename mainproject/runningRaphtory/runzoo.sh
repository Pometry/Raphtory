docker ps -aq --no-trunc | xargs docker rm > /dev/null 2>&1
docker run -p 2181:2181 -p 2888:2888 -p 3888:3888 -d --name="zookeeper" miratepuffin/raphtory-zookeeper:latest
echo "Docker now running at port 2181"