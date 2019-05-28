
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions


# logging
logging.getLogger().setLevel(logging.INFO)


# create a pipeline, read data from csv
pipeline_options = beam.Pipeline(options=PipelineOptions())


# extract product-type and price
class ExtractProductTypePrice(beam.DoFn):

    def process(self, element, *args, **kwargs):
        line = element.split(',')
        return [(line[2], float(line[3]))]


transactions = (pipeline_options
                | "Read Transaction CSV"
                >> beam.io.ReadFromText("gs://spark-dataset-1/datasets/sales/sales_transactions.csv")
                | "Extracting product type and prices"
                >> beam.ParDo(ExtractProductTypePrice())
                | "Grouping by product type"
                >> beam.GroupByKey()
                | "Product Prices by Product Type"
                >> beam.ParDo(lambda (k, v): logging.info("Product Type %s = Prices %s", k, str(v)))
                )


# run
pipeline_options.run().wait_until_finish()




