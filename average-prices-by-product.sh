#!/usr/bin/env bash

# script to run line-count on GCP dataflow

export PROJECT_ID=sandbox-236618
export FILE_NAME=sales-transaction-ops

python sales-transaction-ops.py \
        --project ${PROJECT_ID} \
        --runner DataFlowRunner \
        --staging_location gs://beam-pipelines-123/${FILE_NAME}/staging \
        --temp_location gs://beam-pipelines-123/${FILE_NAME}/temp