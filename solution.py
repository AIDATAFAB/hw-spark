from pyspark.sql import SparkSession

from tests.baseline import Baseline

class Solution:
    TESTS = Baseline.TESTS # CREATE YOUR OWN SQL QUERIES WITH THE SAME RESULTS

    @staticmethod
    def prepare_data(ss: SparkSession):
        Baseline.prepare_data(ss, prefix="") # DELETE THIS LINE AND CREATE YOUR OWN SOLUTION
