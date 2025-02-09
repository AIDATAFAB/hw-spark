from pyspark.sql import SparkSession

from tests.baseline import Baseline

class Solution:
    SCHEMA = 'solution'

    # эти тесты тут для примера, для получения максимальной оценки необходимо написать свои запросы которые будут совпадать по результату с baseline
    # запросы могут использовать любые таблицы, которые вы создадите, а не только 1:1 baseline.
    # все запросы должны быть параметризированы как в Baseline.TESTS.
    TESTS = Baseline.TESTS 

    @staticmethod
    def prepare_data(ss: SparkSession):
        # вы можете создавать в вашей схеме solution любые таблицы.
        # для получения максимальной оценки обязательно наличие комментариев по каждой оптимизации.
        # в этом примере мы просто напрямую берем baseline, что конечно не приведет к хорошему результату

        if not ss.catalog.tableExists('title_basics'):
            ss.read.table('baseline.title_basics').write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable('title_basics')

        if not ss.catalog.tableExists('title_ratings'):
            ss.read.table('baseline.title_ratings').write.format('delta').mode('overwrite').option('overwriteSchema', True).saveAsTable('title_ratings')
