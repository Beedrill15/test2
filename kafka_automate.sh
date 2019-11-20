#!/bin/bash
if [[ $(jps | grep QuorumPeerMain) ]]; then
echo "Zookeeper is running"
else
echo "Zookeeper is not running.  Restarting zookeeper."
bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
fi
if [[ 3 -eq $(jps | grep Kafka | wc -l) ]]; then
echo "All the servers are running"
else
bin/kafka-server-start.sh -daemon config/server.properties
bin/kafka-server-start.sh -daemon config/server-one.properties
bin/kafka-server-start.sh -daemon config/server-two.properties
fi
