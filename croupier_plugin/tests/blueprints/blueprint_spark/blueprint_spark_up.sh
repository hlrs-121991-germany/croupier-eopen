#!/bin/bash
cfy blueprints upload -b $1 blueprint_spark.yaml
# read -n 1 -s -p "Press any key to continue"
# echo ''
cfy deployments create -b $1 -i ./inputs_spark.yaml --skip-plugins-validation $1
# read -n 1 -s -p "Press any key to continue"
# echo ''
cfy executions start -d $1 install

read -p "Press Y to run job " -n 1 -r; echo ""
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cfy executions start -d $1 run_jobs
fi
