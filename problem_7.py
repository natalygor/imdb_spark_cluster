import pyspark.sql.functions as f
from pyspark.sql import SparkSession, Window
from settings import *
import read_write
def problem_7(spark_session):
    """
    solution of problem 7
    :param spark_session: spark session id
    :return:

    """
    f_title_ratings = path_dir_in + '/' + 'title.ratings.tsv.gz'
    f_title_basics = path_dir_in + '/' + 'title.basics.tsv.gz'
    f_result = path_dir_out + '/' + 'problem_7'
    title_ratings = read_write.read_imdb(spark_session, f_title_ratings, schema_title_ratings)
    title_basics = read_write.read_imdb(spark_session, f_title_basics, schema_title_basics)
    df_1 = (title_basics.withColumn('decade', f.array(f.floor(f.col('startYear') / 10) * 10,
                                                      (f.when(f.col('endYear').isNotNull(),
                                                              (f.ceil(f.col('endYear') / 10) * 10 - 1))
                                                      .otherwise((f.floor(f.col('startYear') / 10) * 10 + 9))))))
    df_2 = df_1.select('tconst', 'primaryTitle', 'decade')
    df_3 = df_2.join(title_ratings, df_2.tconst == title_ratings.tconst, 'inner')
    window_dept = Window.partitionBy("decade").orderBy(f.col("averageRating").desc())
    df_4 = df_3.withColumn("top", f.row_number().over(window_dept))
    df_5 = df_4.select('primaryTitle', 'decade', 'averageRating', 'numVotes', 'top').filter(f.col('top') <= 10)
    df_out = df_5.withColumn('decade', df_5.decade.cast(t.StringType()))
    read_write.write_result(f_result, df_out)

