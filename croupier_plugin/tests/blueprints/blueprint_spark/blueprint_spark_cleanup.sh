#!/bin/bash
cfy executions cancel -f $1
echo "Deleting deployment.."
cfy deployments delete -f spark-submit
echo "Deleting blueprint.."
cfy blueprints delete spark-submit
