from pyspark.sql import SparkSession

spark = SparkSession.builder.appname("Moraya").getorcreate()

df = spark.read.csv("amazon",header =True, inferchema=Ture)

df.show()