#!/bin/bash

RED='\033[0;31m'
NC='\033[0m'

if [[ $# -lt 4 ]]
then
	echo "Usage: $0 <bytes for dummy size> <# of messages> <period in second> <address of primary node>"
	echo "       Human readable format is acceptable (i.e., 1K equals 1024)"
	echo "Example: $0 1K 3 0.5 http://localhost:1112"
	echo "Send 3 dummy request messages (1024 bytes for each) 0.5 seconds to http://localhost:1112/req"

	exit
fi

DUMMYSIZE=`numfmt --from=iec $1`
TOTALMSG=$2
PERIOD=$3
ADDR="$4/req"

DUMMYPATH="/tmp/dummyload"

printf "${RED}Send $TOTALMSG dummy request messages ($DUMMYSIZE bytes for each) $PERIOD seconds to $ADDR${NC}\n"

# Create dummy request
# {"clientID":"Client1", "operation":"Op1", "data": "...", "timestamp":120938}
echo "Try to create dummy request"
printf '{"clientID":"Client1", "operation":"Op1", "data":"' > $DUMMYPATH
tr -dc '0-9A-Z' < /dev/urandom | head -c $DUMMYSIZE >> $DUMMYPATH
printf '", "timestamp":120938}' >> $DUMMYPATH
echo "Dummy request created!"
echo ""

for i in `seq 1 $TOTALMSG`
do
	echo "Try to send message $i"
	curl -H "Content-Type: application/json" -X POST -d "@$DUMMYPATH" $ADDR &
	sleep $PERIOD
done
