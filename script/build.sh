#!/bin/bash

set -e

lein clean

lein uberjar

docker build -t onyx-batch-fix:0.1.0 .
