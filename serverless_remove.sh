#!/usr/bin/env bash

cd aggregation-repository
echo Destroying serverless bundle...
serverless remove --verbose;
