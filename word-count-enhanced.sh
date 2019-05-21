#!/usr/bin/env bash

# script to run word-count-enhanced on GCP dataflow

export PROJECT_ID=sandbox-236618
export FILE_NAME=word-count-enhanced

python ${FILE_NAME}.py \
        --project ${PROJECT_ID} \
        --job_name ${FILE_NAME} \
        --input "gs://spark-dataset-1/datasets/words/book.txt" \
        --output "gs://spark-dataset-1/datasets/words/book_GCP" \
        --runner DataFlowRunner \
        --staging_location gs://beam-pipelines-123/${FILE_NAME}/staging \
        --temp_location gs://beam-pipelines-123/${FILE_NAME}/temp