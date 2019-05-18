# beam-sandbox



### How To Start
1. Enable some of the GCP APIs (dataflow api, json api, logging api, biq query api, storage api, datastore api) from GCP console UI
2. Establish environment
   - create a conda `conda create -n beam-sandbox` environment, 
   - activate w/ `conda activate beam-sandbox` and 
   - install `pip install apache-beam[gcp]` 
3. Test environment w/ 
   - `python -m apache_beam.examples.wordcount --output beam/text`, 
   - Then, `cat beam/t*` to see words and counts.
4. Create a bucket for dataflow on GCP Storage, right after creating a GCP Project !
   - Edit `./run-count-dataflow.sh` file and change w/ your `${PROJECT_ID}`
   - Create a bucket named `beam-pipelines-123`
   - Under this folder create folders for every beam file such as `line-count`
        - then, staging and temp folders such as `line-count\staging` and 
        - `line-count\temp` folders
5. Create a dataset bucket `gs://spark-dataset-1` on GCP Storage, and upload `dataset` folder into it. Public bucket level is much better.
6. `export GOOGLE_APPLICATION_CREDENTIALS=PATH_OF_SERVICE_ACCOUNT.json`
   
#### How To Run
1. to run
   - [x] `python line-count.py` on your local (uses DirectRunner), or
   - [x]  Run `./run-count-dataflow.sh` on your local (uses DataFlowRunner) or on a GCP Instance (uses DataFlowRunner)
2. Look Dataflow UI on GCP console and dataflow jobs running.
3. Check logs