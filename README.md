# beam-sandbox



### How To Start
1. Enable some of the GCP APIs (dataflow api, json api, logging api, biq query api, storage api, datastore api) from GCP console UI
2. Establish environment
   - create a conda `conda create -n beam-sandbox` environment, 
   - activate w/ `conda activate beam-sandbox` and 
   - install `pip install apache-beam[gcp]` 
3. Test everything w/ 
   - `python -m apache_beam.examples.wordcount --output beam/text`, 
   - Then, `cat beam/t*` to see words and counts.