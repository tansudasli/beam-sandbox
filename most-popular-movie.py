import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

# set logging level
root = logging.getLogger()
root.setLevel(logging.INFO)

# create a pipeline
p = beam.Pipeline(options=PipelineOptions())

# user-id,movie-id,rating,timestamp
lines = (p
         | "Read Text File : Weather"
         >> beam.io.ReadFromText("datasets/ml-100k/u.data")
         | "Split Data"
         >> beam.Map(lambda line: line.split("\t"))
         )


# the aim is: movie-id or movie-name, rating-count-by-value
# to get movie-name, we need injection into pipeline like spark's broadcasting
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
