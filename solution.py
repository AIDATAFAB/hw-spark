from pyspark.sql import SparkSession

from tests.baseline import Baseline

class Solution:
    SCHEMA = 'solution'

    # эти тесты тут для примера, для получения максимальной оценки необходимо написать свои запросы которые будут совпадать по результату с baseline
    # запросы могут использовать любые таблицы, которые вы создадите, а не только 1:1 baseline.
    # все запросы должны быть параметризированы как в Baseline.TESTS.
    # новые запросы будут добавляться в первые две недели работы над ДЗ, следите за новостями в telegram группе!

    TESTS = [
"""
WITH report AS
(
SELECT 
    explode(split(genres, ',')) AS genre, 
    CAST(numVotes AS INT) as numVotes, 
    CAST(averageRating AS DOUBLE) AS averageRating
FROM title_basics
JOIN title_ratings USING (tconst)
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
        # вы можете создавать в вашей схеме solution любые таблицы.
        # для получения максимальной оценки обязательно наличие комментариев по каждой оптимизации.
        # в этом примере мы просто напрямую берем baseline, что, конечно, не приведет к хорошему результату и даже не побьет baseline более чем на 10%

        for table_name in Baseline.TABLES:
            if not ss.catalog.tableExists(table_name):
                ss.read.table('baseline.' + table_name).createOrReplaceTempView(table_name)
                # ss.read.table('baseline.' + table).write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable(table)
