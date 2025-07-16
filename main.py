import os

from pyspark.sql import SparkSession


os.environ["PYTHON_SPARK"] = ":C/Users/valen/Desktop/Projects/PySpark/venv/Scripts/python.exe"






if __name__ == "__main__":
    spark = SparkSession.builder \
                .appName("MySpark") \
                .getOrCreate()

    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    df.show()

    spark.stop()
    


