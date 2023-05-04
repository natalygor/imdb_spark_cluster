import pyspark.sql.functions as f
from settings import *
import read_write
def problem_2(spark_session):
    """
    solution of problem 2
    :param spark_session: spark session id
    :return:

    """
    f_in = path_dir_in + '/' + 'name.basics.tsv.gz'
    f_out = path_dir_out + '/' + 'problem_2'
    name_basics = read_write.read_imdb(spark_session, f_in, schema_name_basics)
    df_2 = name_basics.select('primaryName', 'birthYear').where((f.col('birthYear') >= 1800) & (f.col('birthYear') < 1900))
    read_write.write_result(f_out, df_2)