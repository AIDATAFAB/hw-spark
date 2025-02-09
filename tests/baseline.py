from pyspark.sql import SparkSession

class Baseline:
    SCHEMA = "baseline"

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
WITH movie_genres AS (
    SELECT
        DISTINCT
        tconst,
        explode(split(genres, ',')) AS genre        
    FROM baseline.title_basics
)
,report AS
(
SELECT 
    genre, 
    CAST(numVotes AS INT) as numVotes, 
    CAST(averageRating AS DOUBLE) AS averageRating
FROM movie_genres
JOIN baseline.title_ratings USING (tconst)
)

SELECT 
    genre, 
    SUM(numVotes) AS numVotes, 
    AVG(averageRating) AS averageRating
FROM report 
WHERE genre = {genre}
GROUP BY genre
"""
    ]

    @staticmethod
    def prepare_data(ss: SparkSession):
        ss.sql("CREATE SCHEMA IF NOT EXISTS baseline").collect()

        for table in Baseline.TABLES:
            target_table_name = "baseline." + table

            if not ss.catalog.tableExists(target_table_name):
                print("CREATE DELTA TABLE " + target_table_name)
                ss.read.table(table + "_csv").write.format("delta").mode("overwrite").option('overwriteSchema', True).saveAsTable(target_table_name)
