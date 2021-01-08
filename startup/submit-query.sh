#/bin/bash
sleep 30
CHECK=$(docker logs raphtory --tail 1)

while [[ "Cluster ready for Analysis" != ${CHECK} ]] ; do
	CHECK=$(docker logs raphtory --tail 1)
	sleep 1
done

echo 'Submitting query...'

JOBID='job-test'
curl -X POST -H "Content-Type: application/json" --data '{"jsonrpc":"2.0","jobID":"'$JOBID'", "analyserName":"com.raphtory.core.analysis.Algorithms.DegreeBasic", "timestamp":1385982060}' 127.0.0.1:8081/ViewAnalysisRequest
echo ''

while [[ "View Analysis manager for $JOBID at 1385982060 finished" != ${CHECK} ]] ; do
	CHECK=$(docker logs raphtory --tail 1)
done

sleep 3
echo '--------------------------------------------------------------'
echo 'You made it! This means Raphtory is ready. Go make some magic!'
echo '--------------------------------------------------------------'

docker-compose -f singlenode.yml down
rm singlenode.yml
rm .env
