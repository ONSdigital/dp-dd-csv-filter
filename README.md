dp-csv-filter
================

Application retrieves a specified CSV file from AWS s3 bucket, and filters it by dimension values.  The output is then written to a new file in an AWS s3 bucket.

The ```/filter``` endpoint accepts HTTP POST request with a FilterRequest body ```{"filePath": "$PATH_TO_FILE$"}```

### Getting started

First grab the code

`go get github.com/ONSdigital/dp-csv-filter`

You will need to have Kafka set up locally. Set the following env variables (the example here uses the default ports)

```
ZOOKEEPER=localhost:2181
KAFKA=localhost:9092
```

Install Kafka:

```
brew install kafka
brew services start kafka
brew services start zookeeper
```

Run the Kafka console consumer
```
kafka-console-consumer --zookeeper $ZOOKEEPER --topic filter-request
```

Run the Kafka console producer
```
kafka-console-producer --broker-list $KAFKA --topic filter-request
```

Run the filter
```
make debug
```

The following curl command will instruct the application attempt to get the specified file from the AWS bucket,
filter it and write the output back to the output file in the bucket
```
curl -H "Content-Type: application/json" -X POST -d '{ "inputUrl": "Open-Data-for-filter.csv", "outputUrl": "Open-Data-filtered.csv", "dimensions": { "NACE": [ "08 - Other mining and quarrying", "1012 - Processing and preserving of poultry meat"], "Prodcom Elements": [ "Work done", "Waste Products"] } }' http://localhost:21100/filter
```

The project includes a small data set in the `sample_csv` directory for test usage.

### Configuration

| Environment variable | Default                 | Description
| -------------------- | ----------------------- | ----------------------------------------------------
| BIND_ADDR            | ":21100"                | The host and port to bind to.
| KAFKA_ADDR           | "http://localhost:9092" | The Kafka address to request messages from.
| S3_BUCKET            | "dp-csv-splitter-1"     | The name of AWS S3 bucket to get the csv files from.
| AWS_REGION           | "eu-west-1"             | The AWS region to use.
| KAFKA_CONSUMER_GROUP | "filter-request"        | The name of the Kafka group to read messages from.
| KAFKA_CONSUMER_TOPIC | "filter-request"        | The name of the Kafka topic to read messages from.

### Contributing

See [CONTRIBUTING](CONTRIBUTING.md) for details.

### License

Copyright ©‎ 2016, Office for National Statistics (https://www.ons.gov.uk)

Released under MIT license, see [LICENSE](LICENSE.md) for details.
