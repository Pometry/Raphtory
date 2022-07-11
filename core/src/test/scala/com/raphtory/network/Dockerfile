FROM nginx:1.22.0
ARG OUTPUT_PORT
ARG INPUT_PORT
RUN apt-get update && apt install -y iproute2
COPY nginx.conf /etc/nginx/nginx.conf
RUN sed -i "s/<OUTPUT_PORT>/$OUTPUT_PORT/g" /etc/nginx/nginx.conf
RUN sed -i "s/<INPUT_PORT>/$INPUT_PORT/g" /etc/nginx/nginx.conf
ENTRYPOINT []
CMD echo "NGINX configuration:" && cat /etc/nginx/nginx.conf && tc qdisc add dev eth0 root netem loss 20% && /docker-entrypoint.sh nginx -g "daemon off;"
#CMD /docker-entrypoint.sh nginx -g "daemon off;"