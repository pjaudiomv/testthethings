#!/usr/bin/env bash

docker-compose -f docker-compose.yaml -f docker-compose.prod.yaml -f docker-compose.secret.yaml down
