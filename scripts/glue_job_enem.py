import sys
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
## @params ['JOB_NAME']
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# A partir daqui, exatamete o mesmo código executado no EMR 

# Ler os dados do enem 2020
enem = (
    spark
    .read
    .format("csv")
    .option("header", True)
    .option("inferSchema", True )
    .option("delimiter", ";")
    .load("s3://datalake-clo-igti-edc/raw-data/enem/year2020/MICRODADOSENEM2020.csv")
)

# Escrita dos dados no formato Parquet 
(
    enem
    .write
    .mode("overwrite")
    .format("parquet")
    .save("s3://datalake-clo-igti-edc/consumer-zone/enem-glue/")
)