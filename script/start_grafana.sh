#!/bin/env bash

Gpath=''
Gport=3000

function echo_ok() {
	echo -e "\033[32m[OK] \033[0m$1\033[0m"
}

function echo_err() {
	echo -e "\033[31m[ERR] \033[0m$1\033[0m"
}

cd $Gpath

cp -f conf/defaults.ini conf/custom.ini
sed -i "s/http_port = 3000/http_port = $Gport/g" conf/custom.ini
./bin/grafana-server -config conf/custom.ini -homepath . &

sleep 1

netstat -natp | fgrep ":$Gport" | fgrep grafana | fgrep LISTEN > /dev/null 2>&1
if [ $? -eq 0 ];then
	echo_ok "Grafana with port[$Gport] start success"
else
	echo_err "Grafana with port[$Gport] start error"
fi

cd -

