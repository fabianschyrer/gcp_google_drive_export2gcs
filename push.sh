#!/usr/bin/env bash
echo "################### Push to DOCKER REGISTRY ... ###################".
gcloud docker -- push <DOCKER_REGISTRY>/google.drive.export.to.gcs:latest