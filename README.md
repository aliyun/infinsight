This is a brief introduction of Inspector, please visit [english wiki](https://yq.aliyun.com/) or [chinese wiki](https://yq.aliyun.com/) if you want to see more details including architecture, data flow, performance and so on.

*  [English document]()
*  [Chinese document](https://github.com/aliyun/infinsight/wiki/Infinsight%E6%8A%80%E6%9C%AF%E6%96%87%E6%A1%A3)
*  [FAQ document](https://yq.aliyun.com/)

# Introduce
---
Infinsight is a **MicroService-Oriented Second Level Monitor System**. We want to provide a monitor system that is easy to use and deploy, so Infinsight has the following features:

1. Infinsight is a microservice-origented agentless monitor system, it collector service status remotely from client(such as mysql-client, redis-client, mongo-client, http-client and so on). So you can deploy it with at lease 1 machine(or VM).
2. Infinsight is a second level monitor system. second level has two means: the one is Second-Level Granularity, the other is Second-Level Data Delay. That is Infinsight can display the service status of target service per second in almost realtime.
3. Infinsight is a high performance monitoring system. Infinsight can service whit at lease thousands of target services with just one machine and query data within few milliseconds.
4. Infinsight is a monitor system with good compression storage capability. It can store trillions level monitoring infomation just using terabytes of storage capacity. Statistics based on actual scenarios, the compression ratio we can achieve is: MongoDB(32:1), Http+Json(80:1)
5. Infinsight is a monitor system that support distributed horizontal expansion. you can just copy Infinshgit to an other machine and run, Infinsight will be automatically senses topology changes and performs balance.

# Simple Usage
---

**Dependency**
> Infinsight depends on **MongoDB** for config management and data persistence, and depends on **Grafana** for data visualization.
> 
> ---
> If you don't have any base enviroument, just run AutoDeploy.sh, it's will be help you to download and config all env you need
> ---

1. Configure your base service
	* 1. make sure **gcc** is exist
	* 2. make sure **golang** is exist and version is greater than 1.10.* 
	
	> if not exist, 'AutoDeploy.sh' will be auto downlaod and deploy golang environment at $DeployDir[default:output]

	* 3. configure IP:PORT of **MongoDB**. 

	> config MongoDB_IP and MongoDB_PORT in Base.cfg
	> 
	> if MongoDB is not exist, 'AutoDeploy.sh' will be auto download and deploy MongoDB at specified IP and PORT at $DeployDir[default:output]
	
	* 4. configure your IP:PORT of **Grafana**. 
	
	> config Grafana_IP and Grafana_PORT in Base.cfg
	> 
	> if Grafana is not exist, 'AutoDeploy.sh' will be auto download and deploy Grafana at specified IP and PORT at $DeployDir[default:output]

2.	Build and Run infinsight
	* 1. build project. 
	
	> config $DeployDir in Base.cfg to specify the building path
	> 
	> run AutoDeploy.sh to build
	 
	* 2. run project. 
	
	> goto the building path($DeployDir)
	> 
	> run start_grafana.sh if you want to use the Grafana downloaded
	> 
	> run start_mongo.sh if you want to use the MongoDB downloaded
	> 
	> run start_inspector.sh to start the Monitor

3. Config new service and instance to monitor
	> cd script/mongodb for adding MongoDB for example
	1. mongo 127.0.0.1:27017/MonitorConfig add_service.js
	2. mongo 127.0.0.1:27017/MonitorConfig add_instance.js
	3. mongo 127.0.0.1:27017/MonitorData create_index.js

4. Config grafana to show monitor data
	1. Add Data Source
	![4.1](https://github.com/aliyun/infinsight/raw/develop/png/4.1%20create%20data%20source.png)
	
	![4.2](https://github.com/aliyun/infinsight/raw/develop/png/4.2%20add%20new.png)

	![4.3](https://github.com/aliyun/infinsight/raw/develop/png/4.3%20choose%20Prometheus.png)
	
	![4.4](https://github.com/aliyun/infinsight/raw/develop/png/4.4%20save%20config.png)
	2. Load Template

	> Import Dashboard: output/grafana_template/mongodb

	![4.5](https://github.com/aliyun/infinsight/raw/develop/png/4.5%20click%20home.png)
	
	![4.6](https://github.com/aliyun/infinsight/raw/develop/png/4.6%20import.png)
	
	![4.7](https://github.com/aliyun/infinsight/raw/develop/png/4.7%20upload%20from%20file.png)
	
	![4.8](https://github.com/aliyun/infinsight/raw/develop/png/4.8%20do%20import.png)
	
	![4.9](https://github.com/aliyun/infinsight/raw/develop/png/4.9%20ok.png)
	
# Add New Service
> if you want to add new service, you can use output/mongodb as template to modify

![](https://github.com/aliyun/infinsight/raw/develop/png/add%20service.png)

1. cp -r output/mongodb output/your_new_service
2. modify add_service.js, change all "mongodb" but "dbType" as the service name you want
3. choose a dbType
4. run this js as "mongo" command: mongo 127.0.0.1:27017/MonitorConfig add_service.js

> support dbType:
> http_json / mysql / redis / mongodb

# Add New instance to Monitor
> if you want to add new service, you can use output/mongodb as template to modify

![](https://github.com/aliyun/infinsight/raw/develop/png/add%20new%20instance.png)

1. change ip:port list as you need
2. run this js as "mongo" command: mongo 127.0.0.1:27017/MonitorConfig add_instance.js

# Query Grammar
1. directly show

all "mongodb" in image below is must be the "service name", never change it.
![](https://github.com/aliyun/infinsight/raw/develop/png/Grammar%201.1.png)

all metrics is just json/bson path or monitor key
![](https://github.com/aliyun/infinsight/raw/develop/png/serverStatus.png)
2. cauculate

if you want to calculate the monitor value, you can add calculate expression in "[]" 

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

![](https://github.com/aliyun/infinsight/raw/develop/png/Grammar%201.2.png)
3. regexp
you can also use regexp to specify a group of metrics, just use reg() function. and the "legend" is the show name of each metric, filed$i means the i's filed. "name" means the last field
![](https://github.com/aliyun/infinsight/raw/develop/png/Grammar%201.3.png)

# Join us
---
We have a WeChat group so that users can join and discuss:<br>
![wechat](https://github.com/aliyun/infinsight/blob/develop/png/wechat.png)
