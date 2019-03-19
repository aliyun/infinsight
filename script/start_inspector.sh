#!/bin/env bash

cd collector_server
cp bin/SuperControl ./bin/SuperControl
sh ./bin/SuperControl collector_server init
sh ./bin/control start
cd -

cd store_server
cp bin/SuperControl ./bin/SuperControl
sh ./bin/SuperControl store_server init
sh ./bin/control start
cd -

cd api_server
cp bin/SuperControl ./bin/SuperControl
sh ./bin/SuperControl api_server init
sh ./bin/control start
cd -

