from utils.helpers import getSparkSession, loadRawData, writeData
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as f

# get local Spark Session

spark = getSparkSession(localThread="local[3]", appName="HelloSparkV1")

bronzeSchema = StructType(
        [
            StructField("Timestamp", StringType(), True),
            StructField("Age", IntegerType(), True),
            StructField("Gender", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("State", StringType(), True),
            StructField("self_employed", StringType(), True),
            StructField("family_history", StringType(), True),
            StructField("treatment", StringType(), True),
            StructField("work_interfere", StringType(), True),
            StructField("no_employees", StringType(), True),
            StructField("remote_work", StringType(), True),
            StructField("tech_company", StringType(), True),
            StructField("benefits", StringType(), True),
            StructField("care_options", StringType(), True),
            StructField("wellness_program", StringType(), True),
            StructField("seek_help", StringType(), True),
            StructField("anonymity", StringType(), True),
            StructField("leave", StringType(), True),
            StructField("mental_health_consequence", StringType(), True),
            StructField("phys_health_consequence", StringType(), True),
            StructField("coworkers", StringType(), True),
            StructField("supervisor", StringType(), True),
            StructField("mental_health_interview", StringType(), True),
            StructField("phys_health_interview", StringType(), True),
            StructField("mental_vs_physical", StringType(), True),
            StructField("obs_consequence", StringType(), True),
            StructField("comments", StringType(), True)    
        ]
    )


surveyDF = loadRawData(spark=spark, pathLocation="./rawData/sample.csv", schemaRaw= bronzeSchema)

#print('the surveyDF shema')
#surveyDF.printSchema()

surveyDFtoBronze = (
    surveyDF
        .withColumn("Timestamp", f.to_timestamp("Timestamp", "yyyy-MM-dd HH:mm:ss")
                    )
        .coalesce(2)
        )


# write DataFrame to Bronze path
writeData(dataFrame= surveyDFtoBronze, pathLocation= "./bronzeData/", formatData="csv")


# Stop Spark Session
spark.stop()