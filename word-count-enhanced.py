import logging
import apache_beam as beam
import re

from apache_beam.options.pipeline_options import PipelineOptions

# set logging level
logging.getLogger().setLevel(logging.INFO)

# create a pipeline
p = beam.Pipeline(options=PipelineOptions())


class ExtractWordCount(beam.DoFn):

    def process(self, element, *args, **kwargs):
        yield (element.lower(), 1)


class SplitWords(beam.PTransform):

    def expand(self, pcoll):
        return (pcoll
                | beam.FlatMap(lambda line: re.findall(r'[A-Za-z]+', line))
                | beam.combiners.Count.PerElement()
                )


lines = (p
         | "Read Text File" >> beam.io.ReadFromText("datasets/words/book.txt")
         | "Get Words" >> SplitWords()
         | "Write output File" >> beam.io.WriteToText("datasets/words/book_output.txt")
         )

(lines
 | "Print Word Counts"
 >> beam.ParDo(lambda (c): logging.info("Lines %s ", c))
 )

p.run().wait_until_finish()
