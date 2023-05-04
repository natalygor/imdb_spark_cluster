import pyspark.sql.functions as f
from settings import *
import read_write
def problem_3(spark_session):
    """
    solution of problem 3
    :param spark_session: spark session id
    :return:

    """
    f_in = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_out = path_dir_out + '/' + 'problem_3'

    title_basics = read_write.read_imdb(spark_session, f_in, schema_title_basics)
    df_3 = title_basics.select('primaryTitle').where((f.col('titleType') == 'movie') & (f.col('runtimeMinutes') > 120))
    read_write.write_result(f_out, df_3)