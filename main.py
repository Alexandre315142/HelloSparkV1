from utils.helpers import getSparkSession

# get local Spark Session

spark = getSparkSession(localThread="local[3]", appName="HelloSparkV1")

surveyDF = (
    spark
    .read
    .format("csv")
    .option("path", "./rawData/sample.csv")
    .option("header","true")
    .load()
)

surveyDF = (
    surveyDF
    .select("Age", "Country", "State")
)

surveyDF.show(4, truncate=False)

# Stop Spark Session
spark.stop()