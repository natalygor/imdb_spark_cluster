import pyspark.sql.functions as f
from settings import *
import read_write
def problem_5(spark_session):
    """
    solution of problem 5
    :param spark_session: spark session id
    :return:

    """
    f_title_akas = path_dir_in + '/' + 'title.akas.tsv.gz'
    f_title_basics = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_out = path_dir_out + '/' + 'problem_5'
    title_akas = read_write.read_imdb(spark_session, f_title_akas, schema_title_akas)
    title_basics = read_write.read_imdb(spark_session, f_title_basics, schema_title_basics)

    df_1 = title_akas.select('titleId', 'region')
    df_2 = title_basics.select('tconst').where(f.col("isAdult") == 1)
    df_3 = df_2.join(df_1, df_1.titleId == df_2.tconst, 'inner')
    df_4 = df_3.groupby('region').count().orderBy('count', ascending=False).limit(100)
    df_5 = df_3.groupby('region').count().orderBy('count', ascending=True).limit(100)
    df_out = df_4.union(df_5)
    read_write.write_result(f_out, df_out)