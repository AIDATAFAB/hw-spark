import subprocess
from pathlib import Path

import random
import sys

import timeit

import pyspark

from delta import configure_spark_with_delta_pip

from tests.baseline import Baseline

from solution import Solution

SELF_PATH = Path(__file__) 
SELF_DIR = SELF_PATH.parent

DEFAULT_NUM_OF_ITERATIONS = 4
RESULT_CONFIDENCE_INTERVAL = 0.1

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
    

def load_spark_tables(ss: pyspark.sql.SparkSession, schema: str, warehouse_path: Path):
    ss.sql("CREATE SCHEMA IF NOT EXISTS " + schema).collect()

    if warehouse_path.exists():
        for item in warehouse_path.glob('*'):
            table_name = schema + "." + item.name
            print("LOAD TABLE " + table_name)
            ss.catalog.createTable(table_name, path=item.absolute().as_posix(), source='delta')

def test_baseline():
    subprocess.run(['cp', SELF_DIR.parent / 'solution.py', SELF_DIR.parent.parent / 'final.py'])

    builder = pyspark.sql.SparkSession.builder.appName("aig") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.enabled", "false")

    ss = configure_spark_with_delta_pip(builder).getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    # loading existing tables
    load_spark_tables(ss, Baseline.SCHEMA, SELF_DIR / 'baseline-warehouse' / 'baseline.db')
    load_spark_tables(ss, Solution.SCHEMA, SELF_DIR.parent / 'spark-warehouse' / f'{Solution.SCHEMA}.db')

    # for file in files:
    #     table_name = file.replace('.tsv.gz', '').replace('.', '_')
    #     ss.read.csv("/tmp/" + file, header=True, sep='\t', nullValue='\\N').createOrReplaceTempView(table_name + "_csv")

    # print("Prepare baseline tables...")
    # Baseline.prepare_data(ss)

    print("Prepare solution tables...")
    start = timeit.default_timer()
    Solution.prepare_data(ss)
    end = timeit.default_timer()
    t = (end - start)
    print("Solution init time (sec):", t)
    
    results = get_test_results(ss)

    cnt_results_achieved  = 0
    for result in results:
        min_baseline_elapsed = float('inf')
        min_solution_elapsed = float('inf')

        for iteration in result:
            print(iteration)
            min_baseline_elapsed = min(min_baseline_elapsed, iteration['baseline_elapsed'])
            min_solution_elapsed = min(min_solution_elapsed, iteration['solution_elapsed'])

        print("min_baseline_elapsed:", min_baseline_elapsed)
        print("min_solution_elapsed:", min_solution_elapsed)

        is_time_result_achieved = (1.0 - (min_solution_elapsed / min_baseline_elapsed)) > RESULT_CONFIDENCE_INTERVAL

        if is_time_result_achieved:
            print("Result achieved!")
            cnt_results_achieved += 1

    cnt_results = len(results)

    print(f"Result: {cnt_results_achieved} / {cnt_results}")
    assert cnt_results == cnt_results_achieved, "Check Baseline Result"
        


def get_test_results(ss: pyspark.sql.SparkSession, num_of_iterations = DEFAULT_NUM_OF_ITERATIONS):
    results = []
    for test_index in range(len(Baseline.TESTS)):
        print(f"TEST # {test_index}")

        print("Baseline:")
        print(Baseline.TESTS[test_index])

        print("Solution:")
        print(Solution.TESTS[test_index])

        params = {}
        params['startYear'] = ss.sql("SELECT DISTINCT CAST(startYear AS INT) FROM baseline.title_basics ORDER BY RAND() LIMIT 1").collect()[0][0]
        params['titleType'] = ss.sql("SELECT DISTINCT titleType FROM baseline.title_basics ORDER BY RAND() LIMIT 1").collect()[0][0]
        params['genre'] = ss.sql("SELECT DISTINCT explode(split(genres, ',')) FROM baseline.title_basics ORDER BY RAND() LIMIT 1").collect()[0][0]
        print("Query parameters:", params)

        test_results = []

        if not run_data_check(ss, test_index, params):
            raise RuntimeError("Data is not the same")

        for iteration in range(num_of_iterations):
            test_result = run_query_perf(ss, test_index, params, iteration)
            print(test_result)
            test_results.append(test_result)            

        results.append(test_results)

    return results

def run_data_check(ss: pyspark.sql.SparkSession, index: int, params: dict):
    print("TEST DATA: START")

    baseline_query = Baseline.TESTS[index]
    baseline_result = ss.sql(baseline_query, **params)

    solution_query = Solution.TESTS[index]
    solution_result = ss.sql(solution_query, **params)

    cnt_baseline_minus_solution = baseline_result.exceptAll(solution_result.select(baseline_result.columns)).count()
    cnt_solution_minus_baseline = solution_result.select(baseline_result.columns).exceptAll(baseline_result).count()

    if cnt_baseline_minus_solution != 0 or cnt_solution_minus_baseline != 0:
        print('BASELINE MINUS SOLUTION:')
        baseline_result.exceptAll(solution_result.select(baseline_result.columns)).show(truncate=False)

        print('SOLUTION MINUS BASELINE:')
        solution_result.select(baseline_result.columns).exceptAll(baseline_result).show(truncate=False)
        return False

    print("TEST DATA: OK")
    return True

def run_query_perf(ss: pyspark.sql.SparkSession, index: int, params: dict, iteration: int):
    print(f"TEST PERF: TEST #{index} ITERATION #{iteration}")

    baseline_query = Baseline.TESTS[index]
    baseline_result = ss.sql(baseline_query, **params)

    solution_query = Solution.TESTS[index]
    solution_result = ss.sql(solution_query, **params)

    start_dt = timeit.default_timer()
    baseline_result.write.format("noop").mode("overwrite").save()
    end_dt = timeit.default_timer()
    baseline_elapsed = (end_dt - start_dt)
    print("Baseline completed:", baseline_elapsed)

    start_dt = timeit.default_timer()
    solution_result.write.format("noop").mode("overwrite").save()
    end_dt = timeit.default_timer()
    solution_elapsed = (end_dt - start_dt)
    print("Solution completed:", solution_elapsed)

    result = {
        'test': index,
        'baseline_elapsed': baseline_elapsed,
        'solution_elapsed': solution_elapsed,
        'baseline_query': baseline_query,
        'solution_query': solution_query,
        'params': params,
        }

    return result
