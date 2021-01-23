#!/bin/sh
#call into the docker container
sudo docker exec -it mongodb bash
host=127.0.0.1
dbname=raphtory

DATABASE_COLLECTIONS=$(mongo $host/$dbname --quiet --eval 'db.getCollectionNames()' | sed 's/,/ /g')

collectionArray=($DATABASE_COLLECTIONS);
size=${#collectionArray[@]} # Size of sortable items
#echo -e "\nArray size: $size"
n=$size

current=$(date +'%m-%d-%Y-%H-%M-%S')
mv outputcollections.txt outputcollections_$(current).txt
touch outputcollections.txt

for ((i=1; i < n-1; i++)); do
  outfilename=${collectionArray[$i]}
  outfilename=${outfilename:1: -1}
#  check if an output file exists, if not then add it to the list to export
  outfilename=${outfilename}&.json
  if [ ! -f ${outfilename} ]; then
    echo ${outfilename} >> outputcollections.txt
    `mongoexport --db $dbname -c ${collectionArray[$i]} --jsonArray --out $outfilename >/dev/null`
  fi
done
exit
