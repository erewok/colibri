#!/bin/bash -eu
echo -e "\nSending test requests to nodes:"
res1=$(curl -iX POST http://localhost:8001/rl/test-client)
echo -e "\nResponse: $res1\n"

res2=$(curl -iX POST http://localhost:8002/rl/test-client)
echo -e "\nResponse: $res2\n"

res3=$(curl -iX POST http://localhost:8003/rl/test-client)
echo -e "\nResponse: $res3\n"


for port in 8001 8002 8003; do
    echo -e "\nChecking quickly on port ${port}..."
    curl -X POST http://localhost:${port}/rl/test-client &
done

echo -e "\nResponse: $res3... sleeping to reset rate limit interval\n"
sleep 4 # Wait for rate limit interval to reset

res4=$(curl -iX POST http://localhost:8002/rl/test-client)
echo -e "\nResponse: $res4\n"

res5=$(curl -iX POST http://localhost:8003/rl/test-client)
echo -e "\nResponse: $res5\n"
