import subprocess
from pathlib import Path

import timeit

import pyspark
from delta import configure_spark_with_delta_pip


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

    for file in files:
        table_name = file.replace('.tsv.gz', '').replace('.', '_')
        ss.read.csv("/tmp/" + file, sep='\t', nullValue='\\N').createOrReplaceTempView(table_name)

    num_of_iterations = 5

    start = timeit.default_timer()
    for i in range(num_of_iterations):
        ss.sql("SELECT COUNT(*) FROM name_basics").collect()
    end = timeit.default_timer()
    t = (end - start) / num_of_iterations
    print(t)
