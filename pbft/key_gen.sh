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

	(
	 # NIST says secp224r1 is as strong as rsa-2048.
	 # Refer to Table 2-1 from:
	 # https://dx.doi.org/10.6028/NIST.SP.800-57pt3r1
	 openssl ecparam -name secp224r1 -genkey -noout -out $PRIVKEYFILE
	 openssl ec -in $PRIVKEYFILE -pubout -out $PUBKEYFILE
	) &
done

wait

printf "${RED}$NUMNODES keys created!${NC}\n"
