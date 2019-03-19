#!/bin/env bash

Gpath=''
Gport=3000

function echo_ok() {
	echo -e "\033[32m[OK] \033[0m$1\033[0m"
}

function echo_err() {
	echo -e "\033[31m[ERR] \033[0m$1\033[0m"
}

pid=`netstat -natp 2>/dev/null | fgrep $Gport | awk '{print $7}' | cut -d '/' -f 1`
kill $pid
echo_ok "Grafana with port[$Gport] pid[$pid] is killed"
