FROM ubuntu:16.04

WORKDIR /app/

COPY ./build/dp-dd-csv-filter .

ENTRYPOINT ./dp-dd-csv-filter
