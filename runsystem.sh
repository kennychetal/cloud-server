#!/bin/bash

host_and_port=$1
sqs_in_name=$2
sqs_out_name=$3
db_write_cap=$4
db_read_cap=$5
db_names=$6
db_proxy=$7
base_port=$8

# echo Write capacity for the DynamoDB table: $db_write_cap
# echo Read capacity for the DynamoDB table: $db_read_cap
# echo List of instance names for your database instances: $db_names

#run frontend.py
./set_aws_env_keys ./frontend.py $host_and_port 8080 DB 3 $base_port localhost $db_proxy $sqs_in_name $sqs_out_name $db_read_cap $db_write_cap $db_names