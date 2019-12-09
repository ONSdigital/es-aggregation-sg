#!/usr/bin/env bash

cd aggregation-deploy-repository
echo Packaging serverless bundle...
serverless package --package pkg
echo Deploying to AWS...
serverless deploy --verbose;
