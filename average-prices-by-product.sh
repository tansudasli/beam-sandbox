#!/usr/bin/env bash

# script to run line-count on GCP dataflow

export PROJECT_ID=sandbox-236618
export FILE_NAME=average-prices-by-product-enhanced

python ${FILE_NAME}.py \
        --project ${PROJECT_ID} \
        --job_name ${FILE_NAME} \
        --runner DataFlowRunner \
        --staging_location gs://beam-pipelines-123/${FILE_NAME}/staging \
        --temp_location gs://beam-pipelines-123/${FILE_NAME}/temp


#python2 average-prices-by-product-enhanced.py \
#          --runner=DataflowRunner \
#          --project=sandbox-236618 \
#          --region=europe-west1 \
#          --staging_location=gs://beam-pipelines-123/average-prices-by-product-enhanced/staging \
#          --temp_location=gs://beam-pipelines-123/average-prices-by-product-enhanced/temp \
#          --max_num_workers=2 \
#          --labels=airflow-version=v1-10-2-composer \
#          --job_name=deploy-averages-prices-by-product-job-4ac95a7a