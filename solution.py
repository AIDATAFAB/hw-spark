from pyspark.sql import SparkSession

from tests.baseline import Baseline

class Solution:
    SCHEMA = 'solution'

    # these tests are here as an example, to get the maximum score you need to write your own queries that will match the baseline in results
    # queries can use any tables that you create, not just 1:1 baseline.
    # all queries must be parameterized as in Baseline.TESTS.
    # new queries will be added in the first two weeks of work on the homework, follow the news in the telegram group!

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
        # you can create any tables in your solution schema.
        # to get the maximum score, it is necessary to have comments on each optimization.
        # in this example, we just take the baseline directly, which, of course, will not lead to a good result and will not even beat the baseline by more than 10%

        for table_name in Baseline.TABLES:
            if not ss.catalog.tableExists(table_name):
                ss.read.table('baseline.' + table_name).createOrReplaceTempView(table_name)
                # ss.read.table('baseline.' + table).write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(table)
