FROM bigtruedata/scala


RUN apt-get update -y && apt-get install build-essential -y && apt-get install tcl8.5 -y
RUN wget http://download.redis.io/releases/redis-stable.tar.gz && tar xzf redis-stable.tar.gz && cd redis-stable && make && make test && make install && cd utils && ./install_server.sh && update-rc.d redis_6379 defaults

RUN apt-get update && apt-get install -y dnsutils && ln -s /opt/docker/aspectjweaver /opt/aspectjweaver

ENTRYPOINT ["/bin/bash"]