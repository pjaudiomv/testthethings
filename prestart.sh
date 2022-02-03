#!/usr/bin/env bash

while ! dijon run-migrations
do
    sleep 5
done
