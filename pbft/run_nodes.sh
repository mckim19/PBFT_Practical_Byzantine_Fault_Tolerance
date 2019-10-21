#!/bin/bash
RED='\033[0;31m'
NC='\033[0m'

if [ $# -lt 1 ]
then
	echo "Usage: $0 <# of nodes>"
	echo "Example: $0 4"
	echo "Try to spawn 4 nodes"
	echo "node Node1 spawned!"
	echo "node Node2 spawned!"
	echo "node Node3 spawned!"
	echo "node Node4 spawned!"
	echo "4 nodes are running"
	echo "(wait)"

	exit
fi

TOTALNODE=$1

if [ ! -d "logs" ]
then
	echo "Logging directory 'logs' does not exist!"
	exit
fi

echo "Try to spawn $TOTALNODE nodes"

echo `awk -v N=$1 -f nodelist.awk /dev/null` > /tmp/node.list

for i in `seq 1 $1`
do
	nodename="Node$i"

	echo "node $nodename spawned!"
	(NODENAME=$nodename; ./main $NODENAME /tmp/node.list 2>&1 > "logs/$NODENAME.log") &
done

echo "$TOTALNODE nodes are running"
echo "(wait)"
wait
