#Comentário para modificar o arquivo .py na aula 4.2 
from pyspark.sql.functions import mean, max, min, col, count 
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ExerciseSpark")
    .getOrCreate()
) 

enem = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", ";")
    .load("s3://datalake-clo-igti-edc/raw-data/enem/year2020/MICRODADOSENEM2020.csv")
)

(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-clo-igti-edc/consumer-zone/enem")
)