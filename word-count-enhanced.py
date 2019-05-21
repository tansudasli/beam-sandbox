import logging
import apache_beam as beam
import re

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from apache_beam.options.pipeline_options import PipelineOptions

# set logging level
logging.getLogger().setLevel(logging.INFO)


# parameterize
class WordCountOptions(PipelineOptions):

    @classmethod
    def _add_argparse_args(cls, parser):
        # super(WordCountOptions, cls)._add_argparse_args(parser)
        parser.add_argument("--input", help="", default="datasets/words/book.txt")
        parser.add_argument("--output", help="", default="datasets/words/book_output.txt")
        # parser.parse_known_args(argv)


# composite transformation
class CountWords(beam.PTransform):

    def expand(self, pcoll):
        return (pcoll
                | beam.FlatMap(lambda line: re.findall(r'[A-Za-z]+', line))
                | beam.Map(lambda word: (word.lower()))
                | beam.combiners.Count.PerElement()
                )


# create a pipeline w/ CLI params
options = PipelineOptions(argv=None)
word_count_options = options.view_as(WordCountOptions)

# Unit Tests
with TestPipeline() as p:
    assert_that(p | beam.Create(["Making inertia", "Overcoming Inertia"])
                  | CountWords(),
                equal_to([("overcoming", 1), ("inertia", 2), ("making", 1)]))

p = beam.Pipeline(options=options)


lines = (p
         | "Read Text File" >> beam.io.ReadFromText(word_count_options.input)
         | "Get Words" >> CountWords()
         | "Write output File" >> beam.io.WriteToText(word_count_options.output)
         )


(lines
 | "Print Word Counts"
 >> beam.ParDo(lambda (c): logging.info("Lines %s ", c))
 )

p.run().wait_until_finish()
