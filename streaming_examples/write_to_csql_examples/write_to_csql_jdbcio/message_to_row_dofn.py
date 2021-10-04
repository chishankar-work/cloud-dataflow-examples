from apache_beam import DoFn
import logging
import datetime
from json import loads
from streaming_examples.write_to_csql_examples.write_to_csql_jdbcio.taxi import Taxi


class MessageToRow(DoFn):

  def __init__(self, *unused_args, **unused_kwargs):
    pass

  def process(self, element, *args, **kwargs):
    m = loads(element)

    try:
      taxi_id = m['taxi_id']
      yield taxi_id, Taxi(id=int(taxi_id), status=str(m['taxi_status']))

    except Exception as exp:

      logging.getLogger().warning(exp)
      yield