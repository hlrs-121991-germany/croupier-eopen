#!/bin/bash
cd blueprint
echo "Uninstalling deployment.."
cfy executions start -d $1 uninstall
echo "Deleting deployment.."
cfy deployments delete $1
echo "Deleting blueprint.."
cfy blueprints delete $1
