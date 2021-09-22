from apache_beam import CombineFn

# CombineFn for handling global accumulator
class GetCurrentJobMetrics(CombineFn):

    # Defining the accmulator init state
    def create_accumulator(self):
        return 0, 0, 0

    # Merges accumulators across all keys together since you can't guarantee all keys are
    # processed on the same machine.
    def merge_accumulators(self, accumulators):

        busy = sum([accum[0] for accum in accumulators])
        available = sum([accum[1] for accum in accumulators])
        idle = sum([accum[2] for accum in accumulators])

        return busy, available, idle

    def add_input(self, accumulator, input):

        return accumulator[0] + input[0], accumulator[1] + input[1], accumulator[2] + input[2]

    def extract_output(self, accumulator):
        return accumulator