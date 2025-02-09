from pyspark.sql import SparkSession

from tests.baseline import Baseline

class Solution:
    TESTS = Baseline.TESTS # CREATE YOUR OWN SQL QUERIES WITH THE SAME RESULTS

    @staticmethod
    def prepare_data(ss: SparkSession):
        # WRITE YOUR INITIALIZATION CODE HERE
        # USE baseline_* tables as a raw data and write to OTHER tables without baseline_ prefixes
        pass
