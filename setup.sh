#!/usr/bin/env bash
echo "################### Building Docker Image ... ###################"
echo "################### 1. create base image for python 3.7 build"
docker build -t google.drive.export.to.gcs .

if [ $? -eq 0 ]
    then
        echo "Docker build & test successfull"
    else
        exit 1
fi

echo "################### Tag to DOCKER REGISTRY ... ###################"
docker tag google.drive.export.to.gcs <DOCKER_REGISTRY>/google.drive.export.to.gcs:latest