#!/bin/env bash

Mpath=''
Mport=27017

function echo_ok() {
	echo -e "\033[32m[OK] \033[0m$1\033[0m"
}

function echo_err() {
	echo -e "\033[31m[ERR] \033[0m$1\033[0m"
}

pid=`netstat -natp 2>/dev/null | fgrep $Mport | awk '{print $7}' | cut -d '/' -f 1`
kill $pid
echo_ok "Mongodb with port[$Mport] pid[$pid] is killed"

