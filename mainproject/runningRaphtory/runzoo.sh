(docker ps -aq --no-trunc | xargs docker rm) >/dev/null
docker run -p 2181:2181 -p 2888:2888 -p 3888:3888 -it --name="zookeeper" miratepuffin/raphtory-zookeeper:latest