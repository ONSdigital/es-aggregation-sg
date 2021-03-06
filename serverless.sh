#!/usr/bin/env bash

cd aggregation-deploy-repository
echo Installing dependancies
serverless plugin install --name serverless-pseudo-parameters
serverless plugin install --name serverless-latest-layer-version
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;
