from pyspark.sql import SparkSession

tables = [
    'name_basics',
    'title_akas',
    'title_basics',
    'title_crew',
    'title_episode',
    'title_principals',
    'title_ratings'
]

class Baseline:
    TESTS = [
    """
    SELECT COUNT(*) AS cnt FROM baseline_name_basics
    """
    ]

    @staticmethod
    def prepare_data(ss: SparkSession, prefix: str = "baseline_"):
        for table in tables:
            if not ss.catalog.tableExists(table):
                table_name = prefix + table
                print("CREATE DELTA TABLE " + table_name)
                ss.read.table(table + "_csv").write.format("delta").mode("overwrite").option('overwriteSchema', True).saveAsTable(table_name)
