# This is a sample Python script.

import os
from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
from problem_1 import *
from problem_2 import *
from problem_3 import *
from problem_4 import *
from problem_5 import *
from problem_6 import *
from problem_7 import *
from problem_8 import *

def main():

    spark_session = (SparkSession.builder
                     .master("local")
                     .appName("task app")
                     .config(conf=SparkConf())
                     .getOrCreate())
    problem_1(spark_session)
    problem_2(spark_session)
    problem_3(spark_session)
    problem_4(spark_session)
    problem_5(spark_session)
    problem_6(spark_session)
    problem_7(spark_session)
    problem_8(spark_session)

if __name__ == "__main__":
    main()
