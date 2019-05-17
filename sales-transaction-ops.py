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

# PCollection format is = id, customer-type, product-type, price
transactions = (pipeline_options
                | "Read Transaction CSV"
                >> beam.io.ReadFromText("gs://spark-dataset-1/datasets/sales/sales_transactions.csv")
                )

print_size(transactions, "Raw Transactions")


# extract product-type and price
class ExtractProductTypePrice(beam.DoFn):

    def process(self, element, *args, **kwargs):
        line = element.split(',')
        return [(line[2], float(line[3]))]


# PCollection format now = product-type, price
productType_price = (transactions
                     | "Extracting product type and price"
                     >> beam.ParDo(ExtractProductTypePrice())
                     )

print_size(productType_price, 'Product Type and Price')


# group prices by product-type, then print
# PCollection format now = product-type, [price1, price2...]
productPrices_by_productType = (productType_price
                                | "Grouping by product type"
                                >> beam.GroupByKey()
                                )

print_size(productPrices_by_productType, "Prices by Product Type")

(productPrices_by_productType
 | "Print Product Types and Prices"
 >> beam.ParDo(lambda (k, v): logging.info("Product Type %s : Prices  %s", k, str(v)))
 )

# find average of prices by product-group, then print
averagePrices_by_productType = (productPrices_by_productType
                                | "Average by product type"
                                >> beam.Map(lambda (k, v): (k, sum(v)/len(list(v))))
                                )

(averagePrices_by_productType
 | "Print Products average prices"
 >> beam.ParDo(lambda (k, v): logging.info("Product type %s: Average %f", k, v))
 )

# write output to a file
# (averagePrices_by_productType
#  | "Write to GCP storage"
#  >> beam.io.WriteToText("gs://spark-dataset-1/datasets/sales/sales_transactions_average_prices_OUT.csv"))

# extract customer-type, then print
# find transaction by customer-type, then print

# run
result = pipeline_options.run()

result.wait_until_finish()
