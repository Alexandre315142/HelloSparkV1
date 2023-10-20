# import library

from pyspark.sql import SparkSession, DataFrame

# function to call local Spark master

def getSparkSession(localThread: str,
                    appName: str) -> SparkSession :
    """_summary_

    Args:
        localThread (str): Number of local VM threads
        appName (str): The name of Spark Application

    Returns:
        SparkSession:
    """
    
    spark = (
        SparkSession
        .Builder()
        .master(localThread)
        .appName(appName)
        .getOrCreate()
    )
    
    return spark