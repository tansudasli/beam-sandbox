import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

# set logging level
root = logging.getLogger()
root.setLevel(logging.INFO)

# create a pipeline
p = beam.Pipeline(options=PipelineOptions())

# source data
lines = (
            p
            | "create sample data"
              >> beam.Create(["Another episode of Star Wars",
                              "A long time ago in a galaxy far far away"])
        )

#
line_count = (
                  lines
                  | "count lines"
                    >> beam.CombineGlobally(beam.combiners.CountCombineFn())
             )

#
(
        line_count
        | "printing to log"
          >> beam.ParDo(lambda (c): logging.info("\n***\ntotal lines: %s \n***", c))
)

result = p.run()

result.wait_until_finish()
