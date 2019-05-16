#!/usr/bin/env bash

# script to run line-count on GCP dataflow

export PROJECT_ID=sandbox-236618

python line-count.py \
        --project ${PROJECT_ID} \
        --runner DataFlowRunner \
        --staging_location gs://beam-pipelines-123/line-count/staging \
        --temp_location gs://beam-pipelines-123/line-count/temp