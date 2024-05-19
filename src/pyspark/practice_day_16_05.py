# Q1. concat column
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
spark = SparkSession.builder.appName("").getOrCreate()
data = [
    (1, 'John', None, 'Doe'),
    (2, 'Alice', 'Ann', 'Smith'),
    (3, 'Mike', None, 'Johnson')
]
schema = ["id", "first_name", "middle_name", "last_name"]
df = spark.createDataFrame(data, schema)
df = df.withColumn(
    "full_name",
    concat_ws(" ", col("first_name"), col("middle_name"), col("last_name"))
)
df.show()
spark.stop()

# Q3. Pivot program
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("").getOrCreate()
data = [
    ("John", "Apple", 10),
    ("Alice", "Apple", 20),
    ("John", "Banana", 12),
    ("Alice", "Banana", 14),
    ("Mike", "Apple", 15),
    ("Mike", "Banana", 17)
]
df = spark.createDataFrame(data, ["salesperson", "product", "quantity"])
pivot_df = df.groupBy("product").pivot("salesperson").sum("quantity")
pivot_df.show()
spark.stop()
