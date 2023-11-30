from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, explode
from pyspark.sql.types import ArrayType, StringType
import os, sys


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# повернення масиву де перший елемент це ім'я автора, 
def process_commit_message(commit_message):
    words = commit_message.split(" ")[:5]
    combinations = [words[i:i+3] for i in range(3)]
    return [" ".join(combination) for combination in combinations]

def foo0(commit_message):
    return process_commit_message(commit_message)[0]

def foo1(commit_message):
    return process_commit_message(commit_message)[1]

def foo2(commit_message):
    return process_commit_message(commit_message)[2]

spark = SparkSession.builder.appName("lab1_main").getOrCreate()

# читаємо файл
df_git = spark.read.json("10K.github.jsonl")

# Фільтруємо записи, де type = 'PushEvent'
filtered_df = df_git.filter(df_git['type'] == 'PushEvent')
df_commits = filtered_df.select(explode("payload.commits").alias("commit"))
df_author_message = df_commits.select("commit.author.name", "commit.message")

generate_text_udf0 = udf(foo0, StringType())
generate_text_udf1 = udf(foo1, StringType())
generate_text_udf2 = udf(foo2, StringType())

# додається новий стовбець "3_grams"
result_df = df_author_message.withColumn("3_grams_1", generate_text_udf0(col("message")))
result_df = result_df.withColumn("3_grams_2", generate_text_udf1(col("message")))
result_df = result_df.withColumn("3_grams_3", generate_text_udf2(col("message")))
result_df = result_df.drop("message")

result_df.printSchema()
result_df.show()

result_df.toPandas().to_csv("output_file.csv", index=False)