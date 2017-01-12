#!/bin/bash

if [[ $(docker inspect --format="{{ .State.Running }}" dp-dd-csv-filter) == "false" ]]; then
  exit 1;
fi
