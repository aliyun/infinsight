#!/bin/env bash

Mpath=''
Mport=27017

function echo_ok() {
	echo -e "\033[32m[OK] \033[0m$1\033[0m"
}

function echo_err() {
	echo -e "\033[31m[ERR] \033[0m$1\033[0m"
}

cd $Mpath

mkdir -p ./data
mkdir -p ./log
./bin/mongod --port $Mport --dbpath=./data --logpath=./log/mongod.log &

sleep 3

netstat -natp | fgrep ":$Mport" | fgrep mongod | fgrep LISTEN > /dev/null 2>&1
if [ $? -eq 0 ];then
	echo_ok "Mongodb with port[$Mport] start success"
else
	echo_err "Mongodb with port[$Mport] start error"
fi

cd -

