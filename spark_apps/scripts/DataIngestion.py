import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import time
from sqlalchemy import create_engine, text

print("\nStarting the Data Migration Job...")

# Record the start time
start_time = time.time()

# Load environment variables from .env file
load_dotenv()

# Get SQL Server credentials from .env
sqlserver_host = os.getenv("SQLSERVER_HOSTNAME")
sqlserver_port = os.getenv("SQLSERVER_PORT")
sqlserver_db = os.getenv("SQLSERVER_DATABASE")
sqlserver_user = os.getenv("SQLSERVER_USERNAME")
sqlserver_password = os.getenv("SQLSERVER_PASSWORD")
sqlserver_driver = os.getenv("SQLSERVER_DRIVER")

# Get MySQL credentials from .env
mysql_host = os.getenv("MYSQL_HOSTNAME")
mysql_port = os.getenv("MYSQL_PORT")
mysql_db = os.getenv("MYSQL_DATABASE")
mysql_user = os.getenv("MYSQL_USERNAME")
mysql_password = os.getenv("MYSQL_PASSWORD")
mysql_driver = os.getenv("MYSQL_DRIVER")

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Migration") \
    .config("spark.jars", "/opt/spark/apps/jars/mysql-connector-java-8.0.30.jar,/opt/spark/apps/jars/mssql-jdbc-12.8.1.jre8.jar") \
    .getOrCreate()

# SQL Server connection details
sqlserver_url = f"jdbc:sqlserver://{sqlserver_host}:{sqlserver_port};encrypt=true;trustServerCertificate=true"
sqlserver_properties = {
    "user": sqlserver_user,
    "password": sqlserver_password,
    "driver": sqlserver_driver
}

# MySQL connection details
mysql_url = f"jdbc:mysql://{mysql_host}:{mysql_port}/{mysql_db}"
mysql_properties = {
    "user": mysql_user,
    "password": mysql_password,
    "driver": mysql_driver
}

# fetch schema list from mysql schema_master table
schema_list = df = spark.read \
                    .format("jdbc") \
                    .option("url", mysql_url) \
                    .option("dbtable", f"{mysql_db}.schema_master") \
                    .option("user", mysql_user) \
                    .option("password", mysql_password) \
                    .option("driver", mysql_driver) \
                    .load().select("schema_name")

sqlserver_connection_url = f"mssql+pymssql://{sqlserver_user}:{sqlserver_password}@{sqlserver_host}:{sqlserver_port}"
# Create an SQLAlchemy engine
engine = create_engine(sqlserver_connection_url, isolation_level="AUTOCOMMIT")

# Iterate over the schema list
for row in schema_list.collect():
    schema_name = row['schema_name']

    # SQL query to create a new database
    create_db_query = text(f"CREATE DATABASE {schema_name};")
    try:
        # Establish connection and execute the query
        with engine.connect() as connection:
            connection.execute(create_db_query)
            print(f"Database '{schema_name}' created successfully.")

    except Exception as e:
        print(f"Database '{schema_name} already exists': {e}")
    
    
    # Example DataFrame to insert
    query = f"""SELECT tm.table_name
                FROM {mysql_db}.table_master tm
                INNER JOIN {mysql_db}.schema_master sm ON sm.schema_mstr_key = tm.schema_mstr_key AND sm.is_active = 1
                WHERE tm.is_active AND sm.schema_name = '{schema_name}' """
    #read data
    tables_list = spark.read \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("query", query) \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .load()
    
    for row in tables_list.collect():
        table_name = row['table_name']
        # Load MySQL table into Spark DataFrame
        df = spark.read \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("dbtable", f"{schema_name}.{table_name}") \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .load()

        df.write \
            .mode("overwrite") \
            .jdbc(sqlserver_url, f"{schema_name}.dbo.{table_name}", properties=sqlserver_properties) 


# Record the end time
end_time = time.time()

# Calculate the duration
duration = round((end_time - start_time)/60, 2)
print("=============================================")
print(f"Job completed in {duration} minutes.")
print("=============================================")
