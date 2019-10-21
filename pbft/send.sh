#!/bin/bash

DUMMYSIZE=1048576 # 1 MiB

# {"clientID":"Client1", "operation":"Op1", "data": "...", "timestamp":120938}
printf '{"clientID":"Client1", "operation":"Op1", "data":"' > /tmp/dummyload
tr -dc '0-9A-Z' < /dev/urandom | head -c $DUMMYSIZE >> /tmp/dummyload
printf '", "timestamp":120938}' >> /tmp/dummyload

for i in `seq 1 100`
do
	curl -H "Content-Type: application/json" -X POST -d @/tmp/dummyload http://localhost:1112/req &
	sleep 1
done
