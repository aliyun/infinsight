#!/bin/env bash

cd mysql
mongo 127.0.0.1:3001/MonitorConfig add_service.js
mongo 127.0.0.1:3001/MonitorConfig add_instance.js
mongo 127.0.0.1:3001/MonitorData create_index.js
cd -

cd redis
mongo 127.0.0.1:3001/MonitorConfig add_service.js
mongo 127.0.0.1:3001/MonitorConfig add_instance.js
mongo 127.0.0.1:3001/MonitorData create_index.js
cd -

cd mongodb
mongo 127.0.0.1:3001/MonitorConfig add_service.js
mongo 127.0.0.1:3001/MonitorConfig add_instance.js
mongo 127.0.0.1:3001/MonitorData create_index.js
cd -

cd inspector
mongo 127.0.0.1:3001/MonitorConfig add_service.js
mongo 127.0.0.1:3001/MonitorConfig add_instance.js
mongo 127.0.0.1:3001/MonitorData create_index.js
cd -

