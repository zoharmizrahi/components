import re
from parallelm.components import SparkSessionComponent


class PiVerifier(SparkSessionComponent):
    PI_VALUE = 3.14159265358979
    DEFAULT_PRECISION = 0.001

    def __init__(self, ml_engine):
        super(self.__class__, self).__init__(ml_engine)

    def _materialize(self, spark, parent_data_objs, user_data):
        precision = self._params.get("precision", PiVerifier.DEFAULT_PRECISION)

        pi_content = parent_data_objs[0]

        if abs(pi_content - PiVerifier.PI_VALUE) <= precision:
            print("INFO: Pi value is in range: {} [precision: {}]".format(pi_content, precision))
        else:
            print("Error: Pi value NOT in range: {} [precision: {}]".format(pi_content, precision))

