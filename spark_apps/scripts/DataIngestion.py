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
#df.show()

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

    # Show the filtered DataFrame
    #schema_tables_df.show()

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
    

    query = f'''SELECT 	tm.table_mstr_key,
                        tm.table_name,
                        sm.schema_mstr_key,
                        sm.schema_name,
                        c.column_name,
                        c.ordinal_position,
                        c.is_nullable,
                        c.data_type,
                        c.character_maximum_length
                FROM {mysql_db}.table_master tm
                INNER JOIN {mysql_db}.schema_master sm ON sm.schema_mstr_key = tm.schema_mstr_key
                INNER JOIN INFORMATION_SCHEMA.COLUMNS c ON c.table_name = tm.table_name AND c.table_schema = sm.schema_name
                WHERE sm.schema_name = '{schema_name}' '''
     
    # print(query)
    # Load MySQL information schema tables into Spark DataFrame
    tables_columns_df = spark.read \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("query", query) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .load()

    # Show the fetched Table Columns DataFrame
    tables_columns_df.show()

    # Check if the combination of table_mstr_key and column_name exists in field_master table
    for row in tables_columns_df.collect():
        table_mstr_key = row["table_mstr_key"]
        column_name = row["COLUMN_NAME"]
        data_type = row["DATA_TYPE"]
        max_length = row["CHARACTER_MAXIMUM_LENGTH"]
        # Check if the data_type exists in data_type_master table
        data_type_df = spark.read \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", "data_type_master") \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .load().filter(col("data_type") == data_type)

        if data_type_df.count() == 0:
            # Define schema for the DataFrame
            data_type_schema = StructType([
                StructField("data_type", StringType(), True)
            ])
            
            # Create DataFrame to insert
            data_type_insert_df = spark.createDataFrame([(data_type,)], data_type_schema)
            
            # Insert data into MySQL table
            data_type_insert_df.write \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("dbtable", "data_type_master") \
                .option("user", mysql_user) \
                .option("password", mysql_password) \
                .option("driver", mysql_driver) \
                .mode("append") \
                .save()
            
            # Get the new data_type_mstr_key
            data_type_mstr_key = spark.read \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("dbtable", "data_type_master") \
                .option("user", mysql_user) \
                .option("password", mysql_password) \
                .option("driver", mysql_driver) \
                .load().filter(col("data_type") == data_type).select("data_type_mstr_key").collect()[0]["data_type_mstr_key"]
        else:
            # Get the existing data_type_mstr_key
            data_type_mstr_key = data_type_df.select("data_type_mstr_key").collect()[0]["data_type_mstr_key"]
        
        # Check if the combination already exists in field_master table
        existing_field_df = spark.read \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", "field_master") \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .load().filter((col("table_mstr_key") == table_mstr_key) & (col("field_name") == column_name))

        if existing_field_df.count() == 0:
            # Define schema for the DataFrame
            field_schema = StructType([
                StructField("table_mstr_key", StringType(), True),
                StructField("field_name", StringType(), True),
                StructField("data_type_mstr_key", StringType(), True),
                StructField("max_length", StringType(), True)
            ])
            
            # Create DataFrame to insert
            field_insert_df = spark.createDataFrame([(table_mstr_key, column_name, data_type_mstr_key,max_length)], field_schema)
            
            # Insert data into MySQL table
            field_insert_df.write \
                .format("jdbc") \
                .option("url", mysql_url) \
                .option("dbtable", "field_master") \
                .option("user", mysql_user) \
                .option("password", mysql_password) \
                .option("driver", mysql_driver) \
                .mode("append") \
                .save()