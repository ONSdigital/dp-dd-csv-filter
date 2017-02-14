#!/bin/bash -eux

export BINPATH=$(pwd)/bin
export GOPATH=$(pwd)/go

pushd $GOPATH/src/github.com/ONSdigital/dp-dd-csv-filter
  go build -o $BINPATH/dp-dd-csv-filter
popd
