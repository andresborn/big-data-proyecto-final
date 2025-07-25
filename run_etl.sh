#!/bin/bash
spark-submit \
  --driver-class-path src/postgresql-42.2.14.jar \
  --jars src/postgresql-42.2.14.jar \
  src/etl/main.py
