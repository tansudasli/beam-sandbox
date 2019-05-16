import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# logging
root = logging.getLogger()
root.setLevel(logging.INFO)

# create a pipeline, read data from csv
# sales transaction structure
# id, customer-type, product-type, price

# extract product-type and price

# extract customer-type, then print

# group prices by product-type, then print

# find average of prices by product-group, then print

# find transaction by customer-type, then print

# write output to a file
