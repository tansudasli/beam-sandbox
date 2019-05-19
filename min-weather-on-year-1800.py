import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions

# set logging level
root = logging.getLogger()
root.setLevel(logging.INFO)

# create a pipeline
p = beam.Pipeline(options=PipelineOptions())


class ExtractWeatherData(beam.DoFn):

    def process(self, element, *args, **kwargs):
        yield (element[0], element[2], float(element[3])/10)


# station-id, date, entryType, celsius,,,,
lines = (p
         | "Read Text File : Weather"
         >> beam.io.ReadFromText("datasets/weather/year-1800.txt")
         | "Split Data"
         >> beam.Map(lambda line: line.split(","))
         | "Reduce Columns"
         >> beam.Map(lambda weather: (weather[0], weather[2], float(weather[3])/10))  #both Map and ParDo is ok
         # >> beam.ParDo(ExtractWeatherData())
         | "Filter TMIN"
         >> beam.Filter(lambda weather: "TMIN" in weather[1])
         | "Reduce K,V"
         >> beam.Map(lambda (c): (c[0], c[2]))
         | "Group By Key "
         >> beam.GroupByKey()  # make them iterable to use min()
         | "Get Min Value"
         >> beam.Map(lambda (k, v): (k, min(v)))
         # | "Write output File"
         # >> beam.io.WriteToText("datasets/words/book_output.txt")
         )


(lines
 | "Print Weather Data"
 >> beam.ParDo(lambda (c): logging.info("Lines %s ", c))
 )

(lines
 | "Print Line Count"
 >> beam.CombineGlobally(beam.combiners.CountCombineFn())
 | "Print line count for %s"
 >> beam.ParDo(lambda (c): logging.info("\nTotal line count = %s \n", c))
 )

p.run().wait_until_finish()
