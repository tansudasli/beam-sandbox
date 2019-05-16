import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# print the number of size
def print_size(pcoll, pname):
    (
            pcoll
            | "Counting lines for %s" % pname
              >> beam.CombineGlobally(beam.combiners.CountCombineFn())
            | "Print line count for %s" % pname
              >> beam.ParDo(lambda (c): logging.info("\nTotal lines in %s = %s \n", pname, c))
    )


# logging
root = logging.getLogger()
root.setLevel(logging.INFO)

# create a pipeline, read data from csv
# sales transaction structure
# id, customer-type, product-type, price

pipeline_options = beam.Pipeline(options=PipelineOptions())

transactions = (pipeline_options
                | "Read Transaction CSV"
                  >> beam.io.ReadFromText("gs://spark-dataset-1/datasets/sales/sales_transactions.csv")
                )


print_size(transactions, "Raw Transactions")


# extract product-type and price

# extract customer-type, then print

# group prices by product-type, then print

# find average of prices by product-group, then print

# find transaction by customer-type, then print

# write output to a file

result = pipeline_options.run()

result.wait_until_finish()
