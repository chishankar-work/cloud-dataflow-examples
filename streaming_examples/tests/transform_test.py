import logging, sys
from typing import List
import apache_beam as beam
import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import coders
from apache_beam.io.jdbc import WriteToJdbc
from streaming_examples.write_to_csql_examples.write_to_csql_jdbcio.message_to_row_dofn import MessageToRow, Taxi
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing import util


class MessageToRowDoFnTest(unittest.TestCase):

  @classmethod
  def setUpClass(self):
    # Create test pipeline
    # Initialize Pipeline with options
    self.sample_taxi_data = [
        '{"taxi_id": 120, "taxi_status": "idle", "status_time": "2021-09-28 20:03:29.864106"}',
        '{"taxi_id": 121, "taxi_status": "idle", "status_time": "2021-09-28 20:03:29.864194"}',
        '{"taxi_id": 122, "taxi_status": "idle", "status_time": "2021-09-28 20:03:29.864274"}',
        '{"taxi_id": 123, "taxi_status": "busy", "status_time": "2021-09-28 20:03:29.864354"}',
        '{"taxi_id": 124, "taxi_status": "busy", "status_time": "2021-09-28 20:03:29.864440"}',
        '{"taxi_id": 125, "taxi_status": "available", "status_time": "2021-09-28 20:03:29.864520"}'
    ]

  @classmethod
  def getMessageToRowExpectedOutput(self, taxi_data) -> List[str]:
    expected_taxi_data = []
    for d in taxi_data:
      t = json.loads(d)
      expected_taxi_data.append(Taxi(id=t['taxi_id'], status=t['taxi_status']))
    return expected_taxi_data

  def test_message_to_row_output(self):

    input = [
        '{"taxi_id": 120, "taxi_status": "idle", "status_time": "2021-09-28 20:03:29.864106"}',
        '{"taxi_id": 122, "taxi_status": "idle", "status_time": "2021-09-28 20:03:29.864274"}'
    ]
    expected_taxi_output = self.getMessageToRowExpectedOutput(input)

    p = TestPipeline()
    coll = p | util.Create(input)
    util.assert_that(coll, util.is_not_empty(), label=f"Not_Empty: {coll}")

    taxi_rows = coll | util.ParDo(MessageToRow())
    util.assert_that(taxi_rows, util.equal_to(expected_taxi_output))

    # grouped_taxi_rows = taxi_rows | util.CoGroupByKey()
    # util.assert_that(grouped_taxi_rows, util.equal_to(taxi_rows))

    p.run()


class JdbcConnectionTest(unittest.TestCase):

  @classmethod
  def setUpClass(cls):
    # Create test pipeline
    cls.sample_taxi_data = [{
        "taxi_id": 120,
        "taxi_status": "idle",
        "status_time": "2021-09-28 20:03:29.864106"
    }, {
        "taxi_id": 121,
        "taxi_status": "idle",
        "status_time": "2021-09-28 20:03:29.864194"
    }, {
        "taxi_id": 122,
        "taxi_status": "idle",
        "status_time": "2021-09-28 20:03:29.864274"
    }, {
        "taxi_id": 123,
        "taxi_status": "busy",
        "status_time": "2021-09-28 20:03:29.864354"
    }, {
        "taxi_id": 124,
        "taxi_status": "busy",
        "status_time": "2021-09-28 20:03:29.864440"
    }, {
        "taxi_id": 125,
        "taxi_status": "available",
        "status_time": "2021-09-28 20:03:29.864520"
    }]
    # Create test pipeline
    pipeline_options = PipelineOptions()
    coders.registry.register_coder(Taxi, coders.RowCoder)

    # Initialize Pipeline with options
    cls.p = TestPipeline() | beam.Create(cls.sample_taxi_data) | beam.ParDo(
        MessageToRow()).with_output_types(Taxi)

  @classmethod
  def tearDownClass(cls) -> None:
    return super().tearDownClass()

  def test_write_to_jdbc(self):
    mysql_driver = "com.mysql.cj.jdbc.Driver"

    jdbc_url = f"jdbc:mysql://172.29.0.4:3306/TaxiLogs?useUnicode=true&useJDBCCompliantTimeZoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

    output = self.p | "WriteToJDBC" >> WriteToJdbc(
        driver_class_name=mysql_driver,
        jdbc_url=jdbc_url,
        username='dataflow-user',
        password='password',
        table_name='taxi_logs',
        expansion_service=f"localhost:8097",
        connection_init_sqls=["SET time_zone='-5:00'"],
    )

    unittest.TestCase.assertTrue(self, util.is_not_empty(),
                                 "Could could connect to DB")


if __name__ == '__main__':
  logging.basicConfig(stream=sys.stderr)
  logging.getLogger("MessageToRowDoFnTest.test_message_to_row_output").setLevel(
      logging.DEBUG)

  unittest.main()
