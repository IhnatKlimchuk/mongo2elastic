#!/bin/bash
echo "sleeping for 10 seconds"
sleep 10

echo mongo_setup.sh time now: `date +"%T" `
mongo --host mongodb1:27017 <<EOF
  var cfg = {
    "_id": "rs0",
    "version": 1,
    "members": [
      {
        "_id": 0,
        "host": "mongodb1:27017",
        "priority": 2
      },
      {
        "_id": 1,
        "host": "mongodb2:27017",
        "priority": 0
      },
      {
        "_id": 2,
        "host": "mongodb3:27017",
        "priority": 0
      }
    ]
  };
  rs.initiate(cfg);
EOF