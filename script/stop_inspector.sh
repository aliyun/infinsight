#!/bin/env bash

cd collector_server
sh ./bin/control stop
cd -

cd store_server
sh ./bin/control stop
cd -

cd api_server
sh ./bin/control stop
cd -

