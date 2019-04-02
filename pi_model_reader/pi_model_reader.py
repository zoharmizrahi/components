import re
from parallelm.components import SparkSessionComponent


class PiModelReader(SparkSessionComponent):
    DEFAULT_PRECISION = 0.001

    def __init__(self, ml_engine):
        super(self.__class__, self).__init__(ml_engine)

    def _materialize(self, spark, parent_data_objs, user_data):
        precision = self._params.get("precision", PiModelReader.DEFAULT_PRECISION)

        with open(self._params['input-pi-model'], "r") as f:
            model_content = f.read()

        float_numbers = re.findall("\d+\.\d+", model_content)
        if not float_numbers:
            raise Exception("Pi number not found in model!")

        self._logger.info("Read Pi value from model: {}".format(float_numbers))

        return [float(float_numbers[0])]
