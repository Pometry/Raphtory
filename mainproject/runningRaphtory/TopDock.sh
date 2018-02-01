#!/usr/bin/env bash 
PST=$(docker ps) 
echo $PST > logs/monidockerPS.txt
(while [ true ]
do
DATE=$(date)
DOCK=$(docker stats --no-stream)
echo -e "$DATE \n $DOCK"
sleep 1
done)  > logs/monidocker.txt