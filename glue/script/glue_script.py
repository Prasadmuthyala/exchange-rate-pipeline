
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
import boto3
# Initialize boto3 Glue client to list tables
client = boto3.client('glue')

# Step 1: Specify your database name
database_name = 'exchangerawdb'

# Step 2: List all tables in the specified database
response = client.get_tables(DatabaseName=database_name)

# Step 3: Filter tables by pattern (e.g., starts with 'results')
matching_tables = [table['Name'] for table in response['TableList'] if table['Name'].startswith("results")]

# Check if we have matching tables
if matching_tables:
    # Assuming you want to use the first matching table
    table_name = matching_tables[0]
    print(f"Selected table: {table_name}")
else:
    print("No matching table found.")

# Step 4: Create a dynamic frame using the selected table
dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name
)

# Step 5: Show the data (for inspection)
dyf.printSchema()
df = dyf.toDF()
df.show()
from pyspark.sql.functions import from_json, col,explode
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, MapType

# Define the schema for the JSON structure inside the "body" column
schema = StructType([
    StructField("conversion_rates", MapType(StringType(), DoubleType())),
    StructField("time_last_update_utc", StringType()),
    StructField("time_next_update_utc", StringType()),
    StructField("timestamp", StringType())
])

# Parse the JSON string in the "body" column
df_parsed = df.withColumn("parsed_body", from_json(col("body"), schema))

# Now you can select the parsed columns and display them
# df_parsed.select("parsed_body").show(truncate=False)

# Select and extract specific fields
df_flattened = df_parsed.select(
    col("parsed_body.conversion_rates").alias("conversion_rates"),
    col("parsed_body.time_last_update_utc").alias("time_last_update_utc"),
    col("parsed_body.time_next_update_utc").alias("time_next_update_utc"),
    col("parsed_body.timestamp").alias("timestamp")
)

# Now let's look at the data
# df_flattened.show(truncate=False)
# Explode the conversion_rates map into separate rows
df_exploded = df_flattened.select(
    col("time_last_update_utc"),
    col("time_next_update_utc"),
    col("timestamp"),
    explode(col("conversion_rates")).alias("currency", "rate")
)

# Now let's view the exploded data
df_exploded.show(truncate=False)
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

# Define a window specification to order by the 'currency' column
windowSpec = Window.orderBy("currency")

# Add a sequential 'ID' column
df_exploded_with_id = df_exploded.withColumn("ID", row_number().over(windowSpec)).select("ID","currency","rate","timestamp","time_last_update_utc","time_next_update_utc")

# Show the result
df_exploded_with_id.show(truncate=False)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
from pyspark.sql.functions import to_timestamp, date_format,substring

# Assuming the column with the date string is named 'time_last_update_utc'
df_converted = df_exploded_with_id.withColumn(
    "last_update_date", 
    date_format(to_timestamp(col("time_last_update_utc"), "EEE, dd MMM yyyy HH:mm:ss Z"), "dd-MM-yyyy")
).withColumn(
    "next_update_date", 
    date_format(to_timestamp(col("time_next_update_utc"), "EEE, dd MMM yyyy HH:mm:ss Z"), "dd-MM-yyyy")
).withColumn(
    "date", 
    date_format(to_timestamp(substring(col("timestamp"),0,11)), "dd-MM-yyyy")
).select("ID","currency","rate","date","last_update_date","next_update_date")


# Show the result
df_converted.show()

df_converted.write.format("parquet").mode("overwrite").save("s3://exchangerateapi/intermediateTransformedData/")
# s3output = glueContext.getSink(
#   path="s3://bucket_name/folder_name",
#   connection_type="s3",
#   updateBehavior="UPDATE_IN_DATABASE",
#   partitionKeys=[],
#   compression="snappy",
#   enableUpdateCatalog=True,
#   transformation_ctx="s3output",
# )
# s3output.setCatalogInfo(
#   catalogDatabase="demo", catalogTableName="populations"
# )
# s3output.setFormat("glueparquet")
# s3output.writeFrame(DyF)
job.commit()