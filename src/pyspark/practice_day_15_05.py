# Q1. program using explode 
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split
spark = SparkSession.builder.appName("").getOrCreate()
data = [
    ("John", "python,sql"),
    ("Aravind", "Java,SQL,HTML"),
    ("Sridevi", "Python,sql,pyspark")
]
df = spark.createDataFrame(data, ["Name", "Skills"])
df_exploded = df.withColumn("Skill", explode(split(df["Skills"], ",")))
df_exploded.select("Name","Skill").show()

# Q2. program using coalesce
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce
from pyspark.sql.types import StructType, StringType, StructField
spark = SparkSession.builder.appName("").getOrCreate()
schema = StructType([
    StructField("Name1", StringType(), True),
    StructField("Name2", StringType(), True),
    StructField("Name3", StringType(), True)
])
data = [("Aravind", None, None),
        ("John", None, None),
        (None, "Sridevi", None)]
df=spark.createDataFrame(data,schema = schema)
df_non_null = df.select(
    coalesce(col("Name1"),coalesce(col("Name2"), col("Name3"))).alias("Names")
)
df_filtered = df_non_null.filter(col("Names").isNotNull())
df_filtered.show(truncate=False)

# Q3. Student Grade
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, when
spark = SparkSession.builder.appName("").getOrCreate()
data1 = [(101, "Aravind"), (102, "John"), (103, "Sridevi")]
data2 = [("pyspark", 90, 101), ("sql", 70, 101), ("pyspark", 70, 102), ("sql", 60, 102), ("sql", 30, 103), ("pyspark", 20, 103)]
students_df = spark.createDataFrame(data1, ["student_id", "student_name"])
marks_df = spark.createDataFrame(data2, ["course_name", "marks", "student_id"])
average_marks_df = marks_df.groupBy("student_id").agg(avg("marks").alias("percentage"))
results_df = average_marks_df.withColumn(
    "result",
    when(col("percentage") >= 70, "Distinction")
    .when((col("percentage") >= 60) & (col("percentage") < 70), "First Class")
    .when((col("percentage") >= 50) & (col("percentage") < 60), "Second Class")
    .when((col("percentage") >= 40) & (col("percentage") < 50), "Third Class")
    .otherwise("Fail")
)
result_df = results_df.join(students_df, "student_id").select("student_id", "student_name", "percentage", "result")
result_df.show()
spark.stop()
