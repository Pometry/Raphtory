PASSWORD=$1
USER=$2

#cd ..
#sbt docker:publish
#cd runningRaphtory

ssh moe@moe.eecs.qmul.ac.uk "cd ben/raphtory/subprojects/singularity/ && 
	 echo $PASSWORD | sudo -S docker pull quay.io/miratepuffin/cluster &&
     rm -r cluster.img && 
     singularity create --size 256 --force cluster.img &&
     echo $PASSWORD | sudo -S singularity bootstrap cluster.img Singularity &&
     scp cluster.img $USER@login2.hpc.qmul.ac.uk:cluster.img"
 