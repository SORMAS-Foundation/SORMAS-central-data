#!/bin/bash
source .env

# todo: use registry.netzlink.com / dockerhub when images are pushed to it
docker run -d --name align-local-central \
  --env host=${host} \
  --env dbname=${dbname} \
  --env username=${sormas_user} \
  --env password=${password} \
  --mount type=bind,source="$(pwd)"/out/,target=/root/out/ \
  align-local-central:${VERSION}