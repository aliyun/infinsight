#!/bin/env bash

################################################################
#                         Inner Func                          #
################################################################
function echo_ok() {
	echo -e "\033[32m[OK] \033[0m$1\033[0m"
}

function echo_err() {
	echo -e "\033[31m[ERR] \033[0m$1\033[0m"
}

function echo_msg() {
	echo -e "\033[33m[MSG] \033[0m$1\033[0m"
}

function echo_normal() {
	echo -e "\033[0m$1\033[0m"
}

function install_golang() {
	# download
	echo_msg "starting to download $golang_tar_name"
	tar_name=$golang_tar_name
	tar_dir=$DeployDir/tar
	wget -c -P $tar_dir $golang_url
	if [ $? != 0 ];then
		echo_err "download go1.10.3 error"
		return 255
	fi
	echo_msg "download success in [$tar_dir/$tar_name]"

	# untar
	echo_msg "starting to uncompact $tar_name"
	if [ ! -e $DeployDir/$golang_dir_name ];then
		tar -xzf $tar_dir/$tar_name -C $DeployDir
	fi
	echo_msg "uncompact success in [$DeployDir/$golang_dir_name]"

	# set env
	GOROOT=$DeployDir/$golang_dir_name
	PATH=$GOROOT/bin:$PATH
	echo_msg "set GOROOT=$GOROOT]"

	return 0
}

function install_mongodb() {
	# download
	echo_msg "starting to download $mongodb_tar_name"
	tar_name=$mongodb_tar_name
	tar_dir=$DeployDir/tar
	wget -c -P $tar_dir $mongodb_url
	if [ $? != 0 ];then
		echo_err "download $mongodb_tar_name error"
		return 255
	fi
	echo_msg "download success in [$tar_dir/$tar_name]"

	# untar
	echo_msg "starting to uncompact $tar_name"
	if [ ! -e $DeployDir/$mongodb_dir_name ];then
		tar -xzf $tar_dir/$tar_name -C $DeployDir
	fi
	echo_msg "uncompact success in [$DeployDir/$mongodb_dir_name]"

	# set env
	PATH=$DeployDir/$mongodb_dir_name/bin:$PATH

	# reconfig and copy start script
	sed -i "s/^Mpath=.*/Mpath=$mongodb_dir_name/g" script/start_mongo.sh
	sed -i "s/^Mpath=.*/Mpath=$mongodb_dir_name/g" script/stop_mongo.sh
	sed -i "s/^Mport=.*/Mport=$MongoDB_PORT/g" script/start_mongo.sh
	sed -i "s/^Mport=.*/Mport=$MongoDB_PORT/g" script/stop_mongo.sh
	cp script/start_mongo.sh $DeployDir/
	cp script/stop_mongo.sh $DeployDir/

	return 0
}

function install_grafana() {
	# download
	echo_msg "starting to download $grafana_tar_name"
	tar_name=$grafana_tar_name
	tar_dir=$DeployDir/tar
	wget -c --no-check-certificate -P $tar_dir $grafana_url
	if [ $? != 0 ];then
		echo_err "download $grafana_tar_name error"
		return 255
	fi
	echo_msg "download success in [$tar_dir/$tar_name]"

	# untar
	echo_msg "starting to uncompact $tar_name"
	if [ ! -e $DeployDir/$grafana_dir_name ];then
		tar -xzf $tar_dir/$tar_name -C $DeployDir
	fi
	echo_msg "uncompact success in [$DeployDir/$grafana_dir_name]"

	# set env
	PATH=$DeployDir/$grafana_dir_name/bin:$PATH

	# reconfig and copy start script
	sed -i "s/^Gpath=.*/Gpath=$grafana_dir_name/g" script/start_grafana.sh
	sed -i "s/^Gpath=.*/Gpath=$grafana_dir_name/g" script/stop_grafana.sh
	sed -i "s/^Gport=.*/Gport=$Grafana_PORT/g" script/start_grafana.sh
	sed -i "s/^Gport=.*/Gport=$Grafana_PORT/g" script/stop_grafana.sh
	cp script/start_grafana.sh $DeployDir/
	cp script/stop_grafana.sh $DeployDir/

	return 0
}

################################################################
#                       Load Base Config                       #
################################################################
script_full_path=`readlink -f $0`
dir_full_path=`dirname $script_full_path`
conf_full_path=$dir_full_path/Base.cfg
source $conf_full_path

################################################################
#                         Main Process                         #
################################################################
# 1.Check Base Environment
# 1.1 Check deploy env
if [ "x$DeployDir" == "x" ]; then
	echo_err 'invalid params'
	echo_msg "Usage: sh AutoDeploy.sh \$DeployDir"
	exit 255
fi
echo_msg "create DeployDir: $DeployDir"
mkdir -p $DeployDir
if [ $? != 0 ]; then
	echo_err "can't create dir: $DeployDir"
	exit 255
fi
DeployDir=`readlink -f $DeployDir`
mkdir -p $DeployDir/tar
echo_ok 'check deploy env complete'

# 1.2 Check gcc env
$CC -v >/dev/null 2>&1
if [ $? != 0 ];then 
	echo_err "check $CC env error:"
	echo_err "please install $CC manully first"
	$CC -v
	exit 255
fi
echo_ok "check $CC env complete"

# 1.3 Check golang env
$GO version >/dev/null 2>&1
if [ $? == 0 ];then 
	version=`$GO version | cut -d ' ' -f 3`
	if [[ $version != $GO_VERSION ]]; then
		echo_msg 'golang version is not support'
		echo_msg "attempt to install golang environment in DeployDir[$DeployDir]"
		install_golang
		if [ $? != 0 ];then
			echo_err "install golang error"
			exit 255
		fi
	fi
else
	echo_msg 'golang is not exist'
	echo_msg "attempt to install golang environment in DeployDir[$DeployDir]"
	install_golang
	if [ $? != 0 ];then
		echo_err "install golang error"
		exit 255
	fi
fi
echo_ok 'check golang env complete'

# 2.Check MongoDB Environment
# 2.1 Check Mongo Client
$MongoClient --version > /dev/null 2>&1
if [ $? != 0 ];then
	echo_msg 'mongo shell client is not exist'
	echo_msg "attempt to install mongo shell environment in DeployDir[$DeployDir]"
	install_mongodb
	if [ $? != 0 ];then
		echo_err "install mongo shell client error"
		exit 255
	fi
fi
echo_ok 'check mongo shell client complete'
# 2.2 Check Mongo Server
MongoConnStr=''
if [ "x$MongoDB_USERNAME" != "x" ];then
	MongoConnStr="$MongoClient $MongoDB_IP:$MongoDB_PORT/$MongoDB_AUTH_DB -u $MongoDB_USERNAME -p $MongoDB_PASSWORD"
else
	MongoConnStr="$MongoClient $MongoDB_IP:$MongoDB_PORT"
fi
$MongoConnStr -eval \"'ping'\" >/dev/null 2>&1
if [ $? != 0 ];then
	echo_msg 'mongo server is not exist'
	echo_msg "attempt to install mongo server environment in DeployDir[$DeployDir]"
	install_mongodb
	if [ $? != 0 ];then
		echo_err "install mongo server error"
		exit 255
	fi
fi
echo_ok 'check mongo server complete'

# 3.Check Grafana Environment
curl http://$Grafana_USERNAME:$Grafana_PASSWORD@$Grafana_IP:$Grafana_PORT/api/org 2>&1 | fgrep -c '"Main Org.' >/dev/null
if [ $? -ne 0 ];then
	echo_msg 'grafana server is not exist'
	echo_msg "attempt to install grafana server environment in DeployDir[$DeployDir]"
	install_grafana
	if [ $? != 0 ];then
		echo_err "install grafana error"
		exit 255
	fi
fi
echo_ok 'check grafana server complete'

# 4.Check Inspector
# 4.1 Reconfig Inspector Config

sed -i "s/^config_address=.*/config_address=\"$MongoDB_IP:$MongoDB_PORT\/$MongoDB_AUTH_DB\"/g" bin/SuperControl
sed -i "s/^config_username=.*/config_username=\"$MongoDB_USERNAME\"/g" bin/SuperControl
sed -i "s/^config_password=.*/config_password=\"$MongoDB_PASSWORD\"/g" bin/SuperControl

sed -i "s/^mongodb_address=.*/mongodb_address=\"$MongoDB_IP:$MongoDB_PORT\/$MongoDB_AUTH_DB\"/g" bin/SuperControl
sed -i "s/^mongodb_username=.*/mongodb_username=\"$MongoDB_USERNAME\"/g" bin/SuperControl
sed -i "s/^mongodb_password=.*/mongodb_password=\"$MongoDB_PASSWORD\"/g" bin/SuperControl

sed -i "s/^api_server_port=.*/api_server_port=\"$Inspector_APIServer_PORT\"/g" bin/SuperControl
sed -i "s/^store_server_port=.*/store_server_port=\"$Inspector_StoreServer_PORT\"/g" bin/SuperControl
sed -i "s/^collector_server_port=.*/collector_server_port=\"$Inspector_CollectorServer_PORT\"/g" bin/SuperControl

echo_ok 'reconfig inspector config'

# 4.2 Build Inspector Api Server
rm -f bin/*_server
./bin/SuperControl api_server build
mkdir -p $DeployDir/api_server
cp -r bin/ $DeployDir/api_server

# 4.3 Build Inspector Store Server
rm -f bin/*_server
./bin/SuperControl store_server build
mkdir -p $DeployDir/store_server
cp -r bin/ $DeployDir/store_server

# 4.4 Build Inspector Collector Server
rm -f bin/*_server
./bin/SuperControl collector_server build
mkdir -p $DeployDir/collector_server
cp -r bin/ $DeployDir/collector_server

# 4.5 Clean
rm -f bin/*_server

# 4.6 Copy start and stop script
cp script/start_inspector.sh $DeployDir/
cp script/stop_inspector.sh $DeployDir/
cp -r script/grafana_template/ $DeployDir/
cp -r script/mongodb/ $DeployDir/

