#!/bin/bash

echo "Starting 10 Java ServerNodes..."
echo "To stop them later, run: killall java"

# Ensure we have a database
echo "30" > database.txt

cd maven_cluster
mvn compile

# We will start 10 nodes (ports 50051 to 50060)
# Here we specify -leaders=10 so all 10 become leaders
# and -mutex so they don't corrupt the database.
for i in {0..9}
do
    PORT=$((50051 + i))
    echo "Starting node on port $PORT..."
    mvn exec:java -Dexec.mainClass="com.flashsale.ServerNode" -Dexec.args="$PORT -sync -nodes=10 -leaders=10 -mutex" &
done

echo "All 10 nodes are running in the background!"
echo "Now run: python webapp/app.py -more"
