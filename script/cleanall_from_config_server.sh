#!/bin/env bash

mongo 127.0.0.1:27017/MonitorData -eval "db.dropDatabase()"
mongo 127.0.0.1:27017/MonitorConfig  -eval "db.dropDatabase()"

