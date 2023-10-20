# import library

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


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
    config_ = {
        "spark.sql.shuffle.partitions":3,
        "spark.sql.debug.maxToStringFields": 100
    }
    
    spark = (
        SparkSession 
        .Builder() 
        .appName(appName) 
        .master(localThread)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.debug.maxToStringFields", "1000")
        .getOrCreate()
    )
    
    return spark

# function to load raw data

def loadRawData(spark: SparkSession, 
                pathLocation: str,
                schemaRaw: StructType = None) -> DataFrame:
    """_summary_

    Args:
        spark (SparkSession): _description_
        schemaRaw (StructType): _description_
        pathLocation (str): _description_

    Raises:
        TypeError: _description_

    Returns:
        DataFrame: _description_
    """
    if not isinstance(pathLocation, str):
        raise TypeError(f"Check if {pathLocation} is string type")
    
    if not spark:
        raise Exception("we need Spark session to continue loading data")
    
    if schemaRaw is not None:
        try:
            df = (
            spark
                .read
                .format("csv")
                .option("path", pathLocation)
                .option("header","true")
                .schema(schema= schemaRaw)
                .load()
                )
        except Exception as e:
            print(f"Throw analysis exception {e}")
    else:
        try:
            df = (
            spark
                .read
                .format("csv")
                .option("path", pathLocation)
                .option("header","true")
                .load()
            )
        except Exception as e:
            print(f"Throw analysis exception {e}")
        
    return df

# function to write data 

def writeData(dataFrame: DataFrame,
              pathLocation: str,
              formatData: str)-> None :
    """_summary_

    Args:
        dataFrame (DataFrame): _description_
        pathLocation (str): _description_
        formatData (None): _description_

    Raises:
        TypeError: _description_
    """
    if not isinstance(pathLocation, str) or not isinstance(dataFrame, DataFrame):
        raise TypeError(f"check the type of input parameters")
    
    if formatData not in ("csv", "json", "parquet"):
        raise TypeError(f"the format {formatData} not allowed for this example")
    try:
         (
            dataFrame
                .write
                .format(formatData)
                .option("path", pathLocation)
                .mode("overwrite")
                .save()
        )
    except Exception as e:
        print(f"Throw analysis exception {e}")
            
    