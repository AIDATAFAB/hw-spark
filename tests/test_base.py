import pyspark
from delta import configure_spark_with_delta_pip

def test_baseline():
    builder = pyspark.sql.SparkSession.builder.appName("aig") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.ui.enabled", "false")

    ss = configure_spark_with_delta_pip(builder).getOrCreate()

    ss.sql("SELECT 1 AS x").write.format("delta").mode("overwrite").saveAsTable('test')

    
