import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from dotenv import load_dotenv
from pyspark.sql.functions import col

# Load environment variables from .env file
load_dotenv()

# Get MySQL credentials from .env
mysql_host = os.getenv("MYSQL_HOSTNAME")
mysql_port = os.getenv("MYSQL_PORT")
mysql_db = os.getenv("MYSQL_DATABASE")
mysql_user = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_driver = os.getenv("MYSQL_DRIVER")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("MySQL to Spark DF with .env") \
    .config("spark.jars", "/opt/spark/apps/jars/mysql-connector-java-8.0.30.jar") \
    .getOrCreate()

# MySQL connection details
mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
mysql_properties = {
    "user": mysql_user,
    "password": mysql_password,
    "driver": mysql_driver
}

# Load MySQL table into Spark DataFrame
df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", f"{mysql_db}.schema_master") \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .option("driver", mysql_driver) \
    .load()

# Filter the DataFrame where is_active = 1
filtered_df = df.filter(df.is_active == 1)

df = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", f"{mysql_db}.schema_master") \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .option("driver", mysql_driver) \
    .load().select("schema_mstr_key","schema_name")

# Show the DataFrame
df.show()

schema_names = df.select("schema_name").rdd.flatMap(lambda x: x).collect()


for schema_name in schema_names:
    # Load MySQL information schema tables into Spark DataFrame
    tables_df = spark.read \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", "information_schema.tables") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .load().select("TABLE_SCHEMA", "TABLE_NAME","TABLE_TYPE")

    
    schema_tables_df = tables_df.filter((tables_df.TABLE_SCHEMA == schema_name) & (tables_df.TABLE_TYPE == "BASE TABLE"))

    # Insert table name and schema master key into MySQL table
    for row in schema_tables_df.collect():
        table_name = row["TABLE_NAME"]
        schema_mstr_key = df.filter(df.schema_name == schema_name).select("schema_mstr_key").collect()[0]["schema_mstr_key"]
        #print(table_name, schema_mstr_key)
        
        # Check if the combination already exists
        existing_df = spark.read \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", "table_master") \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .load().filter((col("table_name") == table_name) & (col("schema_mstr_key") == schema_mstr_key))

        if existing_df.count() == 0:
            # Define schema for the DataFrame
            schema = StructType([
                StructField("schema_mstr_key", StringType(), True),
                StructField("table_name", StringType(), True)
            ])
            
            # Create DataFrame to insert
            insert_df = spark.createDataFrame([(schema_mstr_key, table_name)], schema)
            
            # Insert data into MySQL table
            insert_df.write \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("dbtable", "table_master") \
                .option("user", mysql_user) \
                .option("password", mysql_password) \
                .option("driver", mysql_driver) \
                .mode("append") \
                .save()

    # Show the filtered DataFrame
    schema_tables_df.show()