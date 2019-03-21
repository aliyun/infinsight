This is a brief introduction of Inspector, please visit [english wiki](https://yq.aliyun.com/) or [chinese wiki](https://yq.aliyun.com/) if you want to see more details including architecture, data flow, performance and so on.

*  [English document]()
*  [Chinese document](https://github.com/aliyun/infinsight/wiki/Infinsight%E6%8A%80%E6%9C%AF%E6%96%87%E6%A1%A3)
*  [FAQ document](https://yq.aliyun.com/)

# Introduce
---
![preview](https://github.com/aliyun/infinsight/raw/resource/png/readme/preview.png)
---
Infinsight is a **MicroService-Oriented General Monitor System With Second Granularity**.

We aimd at providing a general monitoring system for most of common service(mysql, redis, mongodb etc) and microservice with high precision and high timeliness to help users troubleshooting easily. In addition we alse expect there is a monitoring system should be easily to deploy and use. So we develop Infinsight, here are some basic feature of it:

1. Infinsight is a **Agentless** monitor system, it's collecte status of target server remotely with local client(such as mysql-client, redis-client, mongo-client, http-client etc.). So you can deploy it with at lease 1 machine(or VM).
2. Infinsight is a **Quasi-Realtime** monitor system. it's has two means: the one is **Second Precision**, the other is **Second Timeliness**. That is Infinsight can display the service status of target service per second in almost realtime.
3. Infinsight is a **High Performance** monitoring system. Infinsight can service whit at lease thousands of target services with just one machine and query data within few milliseconds.
4. Infinsight is a monitor system with **High Compression Ratio**. It can store trillions records just using terabytes of storage capacity. Statistics based on actual scenarios, the compression ratio we can achieve is: MongoDB(32:1), Http+Json(80:1)
5. Infinsight is a **Schemaless** monitor system. you don't need to specify which metrics to monitor, we will be get and save all of status. you can just add or delete metric with valid format(json bson or key-value) if you want to add a new metrics, infinsight will automatically senses the change of metrics and add new one.
6. Infinsight is a monitor system that support **Distributed Horizontal Expansion**. you can just copy Infinshgit program to an other machine and start as same configuration, Infinsight will be automatically senses changes of topology and performs balance.

# Simple Usage
---

**Dependency**
> Infinsight depends on **MongoDB** for config management and data persistence, and depends on **Grafana** for data visualization.
> 
> ---
> If you don't have any base enviroument, just run AutoDeploy.sh, it's will be help you to download and config all env you need
> ---

## 1. Build

> 1. Make sure "gcc" command is exist
> 2. Open and update “Base.cfg”， Base.cfg is a bash script. If you don't have "golang","mongodb" or "grafana" env, please don't change anything.
> 	* modify PATH GOROOT GOPATH for "go" command
> 	* modify PATH for "mongo" command
> 	* modify MongoDB_IP MongoDB_PORT and all MongoDB_* to connect an exist MongoDB Server
> 	* modify Grafana_IP Grafana_PORT and all Grafana_* to connect an exist Grafana Server
> 	* modify Inspector\_*_PORT if port confict
> 3. Run "sh AutoDeploy.sh", script will be config your base env and compile you Infinsight, like this
> ![build](https://github.com/aliyun/infinsight/raw/resource/png/readme/build.png)
> ![output-dir](https://github.com/aliyun/infinsight/raw/resource/png/readme/output-dir.png)
> your output directory will be like this:
> 	* api_server, store_server, collector_server is 3 parts of Infinsight
> 	* service_template is all template of all service we support. It's will help you to register your service and instance easily
> 	* go mongodb-linux-x86_64-4.0.6 grafana-6.0.1 is base envirenment
> 	* tar for downloading
> 	* all script(*.sh) is start and stop script for each service

## 2. Run

1. Start Infinshgit
	* cd output
	* sh start_mongo.sh (if you don't have mongo server in your system)
	* sh start_grafana.sh (if you don't have grafana server in your system)
	* sh start_inspector.sh

2. Config Grafana Data Source
	1. Add Data Source
	![4-1](https://github.com/aliyun/infinsight/raw/resource/png/readme/4-1.png)
	
	![4-2](https://github.com/aliyun/infinsight/raw/resource/png/readme/4-2.png)
	we use Prometheus Http API to communicate with Grafana

	![4-3](https://github.com/aliyun/infinsight/raw/resource/png/readme/4-3.png)
	
	Set "Name" as "Infinsight", you can use more standard grafana template easily
		
3. Add New Service
	* cd output/service_template
	* open service.cfg, service.cfg is a bash script
	* config MongoIP and MongoPort
	* config service_name to whatever you want
	* config service_type to one of [mysql, redis, mongodb, http_json]
	* add you instance list as example
	* run "sh register.sh"(make sure "mongo" command exist), in the directory you choose for service_type will be create 4 files: add_instance.js add_service.js create_index.js and grafana.json

4. Load Grafana Template
![](https://github.com/aliyun/infinsight/raw/resource/png/readme/import%20dashboard-1.png)
![](https://github.com/aliyun/infinsight/raw/resource/png/readme/import%20dashboard-2.png)
select output/service_template/mongodb/grafana.json
![](https://github.com/aliyun/infinsight/raw/resource/png/readme/config%20suuccess.png)
then, Infinsight is starting to monitor your service

# Query Grammar
1. directly show
all "mongodb" in image below is must be the "service name", never change it.
![](https://github.com/aliyun/infinsight/raw/resource/png/readme/Grammar%201.1.png)
all metrics is just json/bson path or monitor key
![](https://github.com/aliyun/infinsight/raw/resource/png/readme/serverStatus.png)

2. cauculate
if you want to calculate the monitor value, you can add calculate expression in "[]" 
![](https://github.com/aliyun/infinsight/raw/resource/png/readme/Grammar%201.2.png)

> calculate function:
> 
> 1. arrayDiff($i)
> 2. arrayAdd($i, $j)
> 3. arraySub($i, $j)
> 4. arrayMul($i, $j)
> 5. arrayDiv($i, $j)
> 6. arrayMod($i, $j)
> 7. arrayDigitAdd($i, num)
> 8. arrayDigitSub($i, num)
> 9. arrayDigitMul($i, num)
> 10. arrayDigitDiv($i, num)
> 11. arrayDigitMod($i, num)
> 
> variable of params $i mean the i's line of metrics
> for example: $1 means "mongodb|network|bytesIn"
> 
> special variable $0 means "each metric", and it's always used in arrayDiff($0) function 

3. regexp
you can also use regexp to specify a group of metrics, just use reg() function. and the "legend" is the show name of each metric, filed$i means the i's filed. "name" means the last field
![](https://github.com/aliyun/infinsight/raw/resource/png/readme/Grammar%201.3.png)

# Join us
---
We have a WeChat group so that users can join and discuss:<br>
![wechat](https://github.com/aliyun/infinsight/raw/resource/png/readme/wechat.png)
