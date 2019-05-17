#!/usr/bin/env bash

# script to run line-count on GCP dataflow

export PROJECT_ID=sandbox-236618
export FILE_NAME=average-prices-by-product

python ${FILE_NAME}.py \
        --project ${PROJECT_ID} \
        --job_name ${FILE_NAME} \
        --runner DataFlowRunner \
        --staging_location gs://beam-pipelines-123/${FILE_NAME}/staging \
        --temp_location gs://beam-pipelines-123/${FILE_NAME}/temp