from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("MockQueryTest") \\
    .getOrCreate()

# Load the CSV file
df = spark.read.option("header", True).csv("/dev/shm/mock_people_big.csv")

# Filter for rows where age > 30
result = df.filter(df.age.cast("int") > 30)

# Collect and print the result to stdout/logs
rows = result.collect()
print("id,name,age")
for row in rows:
    print(f"{row.id},{row.name},{row.age}")

# Optionally write to a file on the driver node
with open("/dev/shm/mock_output_collected.csv", "w") as f:
    f.write("id,name,age\\n")
    for row in rows:
        f.write(f"{row.id},{row.name},{row.age}\\n")

spark.stop()
