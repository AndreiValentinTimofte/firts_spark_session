import os


from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Carica le variabili d'ambiente dal file .env
# Questo renderà disponibili le variabili definite nel file .env come se fossero state impostate nel tuo terminale.
load_dotenv()

if __name__ == "__main__":
    # 1. Leggiamo il percorso di Python dal nostro file .env
    # Usiamo os.getenv() che restituisce None se la variabile non è trovata, evitando errori.
    pyspark_python_path = os.getenv("PYTHON_SPARK")

    spark = SparkSession.builder \
                .appName("MySpark") \
                .config("spark.pyspark.python", pyspark_python_path) \
                .getOrCreate() 

    data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
    df = spark.createDataFrame(data, ["id", "name"])
    
    df.show()

    spark.stop()
    
