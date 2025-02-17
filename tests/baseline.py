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
WITH title_genres AS (
    SELECT
        DISTINCT
        tconst,
        explode(split(genres, ',')) AS genre        
    FROM baseline.title_basics
)
,report AS
(
SELECT 
    TRIM(genre) AS genre, 
    CAST(numVotes AS INT) as numVotes, 
    CAST(averageRating AS DECIMAL(38, 2)) AS averageRating
FROM title_genres
JOIN baseline.title_ratings USING (tconst)
)

SELECT 
    genre, 
    SUM(numVotes) AS numVotes, 
    SUM(averageRating) AS SUM_averageRating
FROM report 
WHERE genre = {genre}
GROUP BY genre
""",
"""
WITH known_for_titles
AS 
(
SELECT
    nb.nconst as nconst,
    explode(split(nb.knownForTitles, ',')) as tconst
FROM baseline.name_basics nb
JOIN baseline.title_principals tp ON nb.nconst = tp.nconst
WHERE tp.category = {category}
)
SELECT 
    COUNT(DISTINCT kft.nconst) AS cnt_distinct_names
FROM known_for_titles kft
JOIN baseline.title_basics tb ON tb.tconst = kft.tconst AND tb.titleType = {titleType}
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
