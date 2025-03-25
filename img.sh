#!/bin/bash

go mod tidy && go mod vendor

tag="v3.14.101"
docker build -t harbor.grandhoo.com/rock/rock:$tag .
docker push harbor.grandhoo.com/rock/rock:$tag
