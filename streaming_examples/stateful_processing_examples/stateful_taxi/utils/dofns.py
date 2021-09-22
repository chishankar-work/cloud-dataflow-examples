from apache_beam import DoFn
from apache_beam.transforms.userstate import BagStateSpec
from apache_beam.coders.coders import TupleCoder, StrUtf8Coder
import datetime
from json import dumps, loads

TIME_FORMAT = '%Y-%m-%d %H:%M:%S.%f'

# Sets the taxi_id as the key
class SetTaxiKeyFn(DoFn):

    def process(self, element):
        # Convert messages to JSON element
        element = loads(element)

        # Sets the taxi_id as key
        yield element['taxi_id'], element

# Sets the formats data
class FormatMetricsData(DoFn):

    def setup(self):
        self.time_format = TIME_FORMAT

    def process(self, element):
        current_metrics = dumps(
            {'busy': element[0],
             'available': element[1],
             'idle': element[2],
             'timestamp': datetime.now().strftime(self.time_format)}).encode('utf-8')

        yield current_metrics

# Stateful ParDo for storing most recent status of taxi_id
class StatefulParDoFn(DoFn):
    TAXI_STATE = BagStateSpec('taxi', TupleCoder((StrUtf8Coder(), StrUtf8Coder())))

    def __init__(self):
        self.BUSY = "busy"
        self.AVAILABLE = "available"
        self.IDLE = "idle"

    def process(self, element, prev_taxi_state=DoFn.StateParam(TAXI_STATE)):

        curr_taxi_status = element[1]['taxi_status']
        curr_taxi_timestamp = element[1]['status_time']

        prev_taxi_info = [x for x in prev_taxi_state.read()]
        update_curr_taxi_flag = False
        new_tup = [0, 0, 0]

        # If there was a previous state listed for this key (job_id)
        if prev_taxi_info:
            prev_taxi_timestamp = prev_taxi_info[0][1]
            prev_taxi_status = prev_taxi_info[0][0]
            # logging.getLogger().info(f"Previous status found for {curr_job_id} -> {prev_job_status}")

            # Compare timestamps if new element is newer than previous job info
            if to_unix_time(curr_taxi_timestamp) - to_unix_time(prev_taxi_timestamp) > 0:

                # Compare previous state to current, update the timestamp
                if prev_taxi_status == curr_taxi_status:
                    # prev_taxi_state.clear()
                    # prev_taxi_state.add((curr_taxi_status, curr_taxi_timestamp))

                    # Don't need to modify anything if the status is the same
                    yield tuple([0, 0, 0])

                # If the state is different and current element is newer
                else:
                    prev_taxi_state.clear()
                    # logging.getLogger().info(f"Previous status for {curr_job_id} is {prev_job_status}, updating to {curr_job_status}")
                    # Update prev_job_state with newer curr_job_info
                    prev_taxi_state.add((curr_taxi_status, curr_taxi_timestamp))
                    update_curr_taxi_flag = True

                    # If previous state is older than current job info, then subtract from respective state
                if prev_taxi_status == self.IDLE:
                    new_tup[2] = -1
                elif prev_taxi_status == self.AVAILABLE:
                    new_tup[1] = -1
                elif prev_taxi_status == self.BUSY:
                    new_tup[0] = -1

            # if current element is not newer then don't update anything return empty values
            else:
                yield tuple([0, 0, 0])

        # If prev_taxi_info is null, then initializes the state and timestamp of new job
        else:
            # logging.getLogger().info(f"Adding new state for job {curr_job_id}")
            prev_taxi_state.add((curr_taxi_status, curr_taxi_timestamp))
            update_curr_taxi_flag = True

        if update_curr_taxi_flag:
            # add value to respective state
            if curr_taxi_status == self.IDLE:
                new_tup[2] = 1
            elif curr_taxi_status == self.AVAILABLE:
                new_tup[1] = 1
            elif curr_taxi_status == self.BUSY:
                new_tup[0] = 1

            # logging.getLogger().info(f"Tuple Value for {curr_job_id}: {new_tup}")
            yield tuple(new_tup)

# Conversion to Unix Time
def to_unix_time(time_str, format=TIME_FORMAT):
    epoch = datetime.utcfromtimestamp(0)
    dt = datetime.strptime(time_str, format)
    return (dt - epoch).total_seconds() * 1000.0