#!/bin/bash

RED='\033[0;31m'
NC='\033[0m'

if [[ $# -lt 1 ]]
then
	echo "Usage: $0 <number of nodes>"
	echo "Example: $0 100"

	exit
fi

NUMNODES=$1
KEYPATH="keys"

# Remove existing keys.
rm -r $KEYPATH

# Create directory for new keys.
mkdir -p $KEYPATH
exitcode=$?
if [[ $exitcode -ne 0 ]] || [[ ! -d $KEYPATH ]]
then
        echo "Key directory $KEYPATH cannot be accessed!"
        exit
fi

for i in `seq 1 $NUMNODES`
do
	PRIVKEYFILE="$KEYPATH/Node$i.priv"
	PUBKEYFILE="$KEYPATH/Node$i.pub"

	(openssl ecparam -name secp384r1 -genkey -noout -out $PRIVKEYFILE
	 openssl ec -in $PRIVKEYFILE -pubout -out $PUBKEYFILE
	) &
done

wait

printf "${RED}$NUMNODES keys created!${NC}\n"
