#!/bin/bash

source ./service.cfg

echo "service_name: $service_name"
echo "service_type: $service_type"

cp -f ./$service_type/add_service.js.template ./$service_type/add_service.js
cp -f ./$service_type/add_instance.js.template ./$service_type/add_instance.js
cp -f ./$service_type/create_index.js.template ./$service_type/create_index.js
cp -f ./$service_type/grafana.json.template ./$service_type/grafana.json
sed -i "s/service_name/$service_name/g" ./$service_type/add_service.js
sed -i "s/service_name/$service_name/g" ./$service_type/add_instance.js
sed -i "s/service_name/$service_name/g" ./$service_type/create_index.js
sed -i "s/service_name/$service_name/g" ./$service_type/grafana.json

n=0
ins_info="ins"$n
while true; do
	ins_id=$n
	eval ins_ip=\${$ins_info[0]}
	eval ins_port=\${$ins_info[1]}
	eval ins_username=\${$ins_info[2]}
	eval ins_password=\${$ins_info[3]}
	if [ ! $ins_ip  -o -z $ins_ip ]; then
		break
	fi

	echo $ins_id
	ins_ip=${ins_ip//./_}
	echo $ins_ip
	echo $ins_port
	echo $ins_username
	echo $ins_password

	items=$items",
			\"$service_name.distribute.$ins_ip:$ins_port\" : { 
				\"pid\" : 0,
				\"hid\" : $ins_id,
				\"host\" : \"$ins_ip:$ins_port\",
				\"username\" : \"$ins_username\",
				\"password\" : \"$ins_password\"
			}
			"

	let n=$n+1
	ins_info="ins"$n
done
items=`echo ${items:1}`
sed -i "15i $items" ./$service_type/add_instance.js

mongo $MongoIP:$MongoPort/MonitorConfig ./$service_type/add_service.js
mongo $MongoIP:$MongoPort/MonitorConfig ./$service_type/add_instance.js
mongo $MongoIP:$MongoPort/MonitorData ./$service_type/create_index.js

