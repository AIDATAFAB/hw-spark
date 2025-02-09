from pyspark.sql import SparkSession

class Baseline:
    TABLES = [
        'name_basics',
        'title_akas',
        'title_basics',
        'title_crew',
        'title_episode',
        'title_principals',
        'title_ratings'
    ]

    TESTS = [
    """
    SELECT COUNT(*) AS cnt FROM baseline_name_basics
    """
    ]

    @staticmethod
    def prepare_data(ss: SparkSession, prefix: str = "baseline_"):
        target_table_name = prefix + table

        for table in Baseline.TABLES:
            if not ss.catalog.tableExists(target_table_name):
                print("CREATE DELTA TABLE " + target_table_name)
                ss.read.table(table + "_csv").write.format("delta").mode("overwrite").option('overwriteSchema', True).saveAsTable(target_table_name)
