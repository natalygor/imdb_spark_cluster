import pyspark.sql.functions as f
from settings import *
import read_write
def problem_1(spark_session):
    """
    solution of problem 1
    :param spark_session: spark session id
    :return:

    """
    f_in = path_dir_in + '/' + 'title.akas.tsv.gz'
    f_out = path_dir_out + '/' + 'problem_1'
    title_akas = read_write.read_imdb(spark_session, f_in, schema_title_akas)
    df_1 = title_akas.select('title').where(f.col('region') == 'UA')
    read_write.write_result(f_out,df_1)
