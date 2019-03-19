#!/bin/bash

source ./service.cfg

echo "service_name: $service_name"
echo "service_type: $service_type"

rm -f ./$service_type/add_service.js
rm -f ./$service_type/add_instance.js
rm -f ./$service_type/create_index.js
rm -f ./$service_type/grafana.json

mongo $MongoIP:$MongoPort/MonitorConfig -eval "db.meta.remove({\"key_unique\" : \"$service_name\"})"
mongo $MongoIP:$MongoPort/MonitorConfig -eval "db.taskList.remove({\"key_unique\" : \"$service_name\"})"
mongo $MongoIP:$MongoPort/MonitorConfig -eval "db.dict_server.remove({\"key_unique\" : \"$service_name\"})"
mongo $MongoIP:$MongoPort/MonitorData -eval "db.$service_name.drop()"

