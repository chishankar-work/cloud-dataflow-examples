import logging
from apache_beam import Pipeline, io, ParDo, coders, Map
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.portability.api.beam_runner_api_pb2 import AccumulationMode
from apache_beam.transforms.core import GroupByKey, WindowInto
from apache_beam.transforms.trigger import AfterAny, AfterCount, AfterProcessingTime
from apache_beam.transforms.window import FixedWindows
from typing import Tuple
from streaming_examples.write_to_csql_examples.csql_example_utils.custom_options import CustomBeamOptions
from streaming_examples.write_to_csql_examples.write_to_csql_jdbcio.message_to_row_dofn import MessageToRow
from streaming_examples.write_to_csql_examples.write_to_csql_jdbcio.taxi import Taxi


def run(argv=None):
  logging.getLogger().setLevel(logging.DEBUG)
  pipeline_options = PipelineOptions()
  max_window_length_time_seconds = 10
  max_window_length_size_elements = 100

  # Initialize Pipeline
  p = Pipeline(options=pipeline_options)

  # Get custom options
  pipeline_config = PipelineOptions().view_as(CustomBeamOptions)

  # com.mysql.jdbc.Driver is deprecated.
  mysql_driver = "com.mysql.cj.jdbc.Driver"

  jdbc_url = f"jdbc:mysql://{pipeline_config.mysql_ip}:{pipeline_config.mysql_port}/{pipeline_config.mysql_db}?useUnicode=true&useJDBCCompliantTimeZoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

  # Register custom coder
  coders.registry.register_coder(Taxi, coders.RowCoder)

  # Read, Convert, and WindowInto
  taxi_rows = (
      p | "ReadFromPubSub" >>
      io.ReadFromPubSub(subscription=pipeline_config.input_subscription) |
      'TaxiConversionAndKeying' >> ParDo(MessageToRow()).with_output_types(
          Tuple[int, Taxi]) | 'FixedWindow' >> WindowInto(
              windowfn=FixedWindows(max_window_length_time_seconds),
              trigger=AfterAny(AfterCount(max_window_length_size_elements),
                               AfterProcessingTime(100)),
              accumulation_mode=AccumulationMode.DISCARDING).with_output_types(
                  Tuple[int, Taxi]))

  grouped_taxi_rows = taxi_rows | 'GroupTaxiById' >> GroupByKey().with_output_types(Tuple[int, Taxi])

  # Adding this GBK ensures that I group all the taxi's with the same taxi_id and all that work goes to the same worker.
  # This GBK currently takes the very first element in the window. If you want to just output the most recent update per taxi per window I would checkout:
  # https://beam.apache.org/documentation/transforms/python/aggregation/latest/
  extract_taxis = grouped_taxi_rows | 'ExtractTaxiObjects' >> Map(
      lambda kv: kv[1][0]).with_output_types(Taxi)

  # Write to CloudSQL
  extract_taxis | 'WriteToMySql:jdbc' >> WriteToJdbc(
      driver_class_name=mysql_driver,
      jdbc_url=jdbc_url,
      username=pipeline_config.mysql_user,
      password=pipeline_config.mysql_pass,
      table_name=pipeline_config.mysql_table,
      expansion_service=f"localhost:{pipeline_config.mysql_expansion_service}",
      connection_init_sqls=["SET time_zone='-5:00'"],
  )

  p.run()
