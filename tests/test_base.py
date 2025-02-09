import subprocess
from pathlib import Path

import timeit

import pyspark
from delta import configure_spark_with_delta_pip

from tests.baseline import Baseline

from solution import Solution


files = [
    'name.basics.tsv.gz',
    'title.akas.tsv.gz',
    'title.basics.tsv.gz',
    'title.crew.tsv.gz',
    'title.episode.tsv.gz',
    'title.principals.tsv.gz',
    'title.ratings.tsv.gz'
]

def download_datasets():
    for file in files:
        subprocess.run(["wget", "https://datasets.imdbws.com/" + file, "-nc", "-O", "/tmp/" + file])    
    
def test_baseline():
    download_datasets()

    builder = pyspark.sql.SparkSession.builder.appName("aig") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.enabled", "false")

    ss = configure_spark_with_delta_pip(builder).getOrCreate()

    # loading existing tables
    warehouse_path = Path('./spark-warehouse/')
    if warehouse_path.exists():
        for item in warehouse_path.glob('*'):
            print("LOAD TABLE " + item.name)
            ss.catalog.createTable(item.name, path=item.absolute().as_posix())

    for file in files:
        table_name = file.replace('.tsv.gz', '').replace('.', '_')
        ss.read.csv("/tmp/" + file, sep='\t', nullValue='\\N').createOrReplaceTempView(table_name + "_csv")

    # prepare baseline tables
    Baseline.prepare_data(ss)

    # prepare solution tables
    start = timeit.default_timer()
    Solution.prepare_data(ss)
    end = timeit.default_timer()
    t = (end - start) / num_of_iterations
    print("Solution init time (sec):", t)
    
    num_of_iterations = 1

    sum_t = 0

    for i in range(num_of_iterations):
        for index, query in enumerate(Baseline.TESTS):
            print(query)
            table_name = "baseline_result_" + str(index)

            if ss.catalog.tableExists(table_name):
                ss.sql("DROP TABLE " + table_name)

            print("Saving results to:", table_name)
            start = timeit.default_timer()
            ss.sql(query).write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(table_name)
            end = timeit.default_timer()
            t = (end - start) / num_of_iterations
            sum_t += t
 
    print("Baseline (seconds):", sum_t)
