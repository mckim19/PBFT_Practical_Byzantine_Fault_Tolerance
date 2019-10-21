#!/bin/bash

if [ ! -d "logs" ]
then
	echo "Logging directory 'logs' does not exist!"
	exit
fi

echo `awk -v N=$1 -f nodelist.awk /dev/null` > /tmp/node.list

for i in `seq 1 $1`
do
	nodename="Node$i"

	echo "$nodename spawned!"
	(NODENAME=$nodename; ./main $NODENAME /tmp/node.list 2>&1 > "logs/$NODENAME.log") &
done

wait
