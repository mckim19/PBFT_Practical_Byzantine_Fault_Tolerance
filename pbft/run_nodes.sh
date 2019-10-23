#!/bin/bash
RED='\033[0;31m'
NC='\033[0m'

if [[ $# -lt 1 ]]
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
NODELISTPATH="/tmp/node.list"
LOGDATE=`date "+%F_%T"`
LOGPATH="logs/$LOGDATE"

mkdir -p $LOGPATH
exitcode=$?
if [[ $exitcode -ne 0 ]] || [[ ! -d $LOGPATH ]]
then
	echo "Logging directory $LOGPATH cannot be accessed!"
	exit
fi

# Build binary file first.
go build main.go
exitcode=$?
if [[ $exitcode -ne 0 ]]
then
	echo "Build Error! (exit code: $exitcode)"
	exit
fi
echo "Build suceeded"

# Update symbolic link for the recent logs.
rm -f "logs/recent" && ln -s $LOGDATE "logs/recent"

echo "Logs are saved in $LOGPATH"
echo ""
echo "Try to spawn $TOTALNODE nodes"

echo `awk -v N=$1 -f nodelist.awk /dev/null` > $NODELISTPATH

for i in `seq 1 $1`
do
	nodename="Node$i"

	echo "node $nodename spawned!"
	(NODENAME=$nodename; ./main $NODENAME $NODELISTPATH 2>&1 > "$LOGPATH/$NODENAME.log") &
done

printf "${RED}$TOTALNODE nodes are running${NC}\n"
echo "(wait)"
wait
