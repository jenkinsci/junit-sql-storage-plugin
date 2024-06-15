#!/usr/bin/env bash

LightGreen='\033[1;32m'
Red='\033[0;31m'
White='\033[97m'
NC='\033[0m'

echo "Building the Jenkins plugin..."
echo -e "${LightGreen}mvn ${White}clean package -P quick-build${NC}"
hpi_file="./target/junit-sql-storage.hpi"
if ! mvn clean package -P quick-build || [ ! -e  "$hpi_file" ]; then
  echo
  echo -e "${Red}Failed to build file ${NC}$hpi_file${Red} check the maven output${NC}"
  exit 1
fi
echo
echo -e "${LightGreen}docker compose up --build -d${NC}"
docker compose up --build -d
echo
# Monitor the Jenkins logs
echo -e "${LightGreen}docker compose logs -f jenkins${NC}"
docker compose logs -f jenkins
