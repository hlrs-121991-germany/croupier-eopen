#!/bin/bash
cfy deployments create -b $1 --skip-plugins-validation $2
# read -n 1 -s -p "Press any key to continue"
# echo ''
cfy executions start -d $2 install

read -p "Press Y to run job " -n 1 -r; echo ""
if [[ $REPLY =~ ^[Yy]$ ]]
then
    cfy executions start -d $2 run_jobs
fi
