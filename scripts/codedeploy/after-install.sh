#!/bin/bash

AWS_REGION=
ECR_REPOSITORY_URI=
GIT_COMMIT=

$(aws ecr get-login --region $AWS_REGION) && docker pull $ECR_REPOSITORY_URI/dp-dd-csv-filter:$GIT_COMMIT
