import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

# set logging level
root = logging.getLogger()
root.setLevel(logging.INFO)

# create a pipeline
p = beam.Pipeline(options=PipelineOptions())


class ExtractWordCount(beam.DoFn):

    def process(self, element, *args, **kwargs):
        yield (element.lower(), 1)


lines = (p
         | "Read Text File"
         >> beam.io.ReadFromText("datasets/words/book.txt")
         | "Get Words"
         >> beam.FlatMap(lambda line: line.split())
         | "Map"
         # >> beam.Map(lambda word: (word, 1))  #both Map and ParDo is ok
         >> beam.ParDo(ExtractWordCount())
         | "Count words"
         >> beam.GroupByKey()
         | "Sum of words"
         >> beam.Map(lambda (k, v): (k, sum(v)))
         | "Write output File"
         >> beam.io.WriteToText("datasets/words/book_output.txt")
         )

(lines
 | "Print Word Counts"
 >> beam.ParDo(lambda (c): logging.info("Lines %s ", c))
 )

p.run().wait_until_finish()
