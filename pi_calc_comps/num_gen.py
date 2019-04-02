import numpy as np
from pyspark.sql import Row

from parallelm.components import SparkSessionComponent


class NumGen(SparkSessionComponent):
    num_samples = 0

    def __init__(self, ml_engine):
        super(self.__class__, self).__init__(ml_engine)

    def _materialize(self, spark, parent_data_objs, user_data):
        num_samples = self._params['num_samples']
        self._logger.info("Num samples: {}".format(num_samples))

        sc = spark.sparkContext

        np_rand = np.random.random
        df = spark.createDataFrame(sc.parallelize([0] * num_samples).map(lambda n: Row(x=np_rand(), y=np_rand())))
        return [df]

