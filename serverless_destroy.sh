#!/usr/bin/env bash

cd aggregation-deploy-repository
echo Destroying serverless bundle...
serverless remove --verbose;