from parallelm.components import SparkSessionComponent

try:
    from parallelm.mlops import mlops
except ImportError:
    pass


class PiCalc(SparkSessionComponent):
    def __init__(self, ml_engine):
        super(self.__class__, self).__init__(ml_engine)

    def _materialize(self, spark, parent_data_objs, user_data):
        if not parent_data_objs:
            self._logger.error("Received an empty DataFrame input!")
            return

        df1 = parent_data_objs[0]
        num_samples1 = df1.count()

        df2 = parent_data_objs[1]
        num_samples2 = df2.count()

        total_samples = num_samples1 + num_samples2
        self._logger.info("Total num samples in RDDs: {}".format(total_samples))

        if "mlops" in dir():
            mlops.set_stat("total-samples", total_samples)

        count1 = df1.filter(PiCalc._inside((df1.x, df1.y))).count()
        count2 = df2.filter(PiCalc._inside((df2.x, df2.y))).count()
        total_samples_inside = count1 + count2

        result = 4.0 * total_samples_inside / total_samples
        print("Pi is roughly {}".format(result))

        self._logger.info("Output model: {}".format(self._params['output-model']))
        with open(self._params['output-model'], "w") as f:
            f.write("Pi is: {}".format(result))

    @staticmethod
    def _inside(t):
        x, y = t[0], t[1]
        return x * x + y * y < 1

