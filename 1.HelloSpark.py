import pyspark

from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Hello Spark").master("local[2]").getOrCreate()

    datalist = [("Rushabh",25),
                ("Uma", 51),
                ("Navin",58),
                ("Bablu", 47)]

    df = spark.createDataFrame(datalist).toDF("Name","Age")

    df.show()

