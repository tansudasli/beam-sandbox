import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

# set logging level
logging.getLogger().setLevel(logging.INFO)

# create a pipeline
p = beam.Pipeline(options=PipelineOptions())

# user-id,movie-id,rating,timestamp
lines = (p
         | "Read Text File : Weather"
         >> beam.io.ReadFromText("datasets/ml-100k/u.data")
         | "Split Data"
         >> beam.Map(lambda line: line.split("\t"))
         | "Get Ratings"
         >> beam.Map(lambda line: (line[2], 1))
         | "Group by Rating"
         >> beam.GroupByKey()
         | "Count by Value"
         >> beam.Map(lambda (k, v): (k, len(v)))
         )


# the aim is: rating, count-by-value
(lines
 | "Print Weather Data"
 >> beam.ParDo(lambda (c): logging.info("Lines %s ", c))
 )

# (lines
#  | "Print Line Count"
#  >> beam.CombineGlobally(beam.combiners.CountCombineFn())
#  | "Print line count for %s"
#  >> beam.ParDo(lambda (c): logging.info("\nTotal line count = %s \n", c))
#  )

p.run().wait_until_finish()
