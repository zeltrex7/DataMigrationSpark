import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from dotenv import load_dotenv
import time
from MSSQLUtilityFunction import execute_query

print("\nStarting the Data Migration Job...")

# Record the start time
start_time = time.time()

# Load environment variables from .env file
load_dotenv()

source_db_name = "mysql"
target_db_name = "mssql"

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
schema_list = spark.read \
    .format("jdbc") \
    .option("url", mysql_url) \
    .option("dbtable", f"{mysql_db}.schema_master") \
    .option("user", mysql_user) \
    .option("password", mysql_password) \
    .option("driver", mysql_driver) \
    .option("fetchsize", "1000") \
    .option("numPartitions", "4") \
    .option("partitionColumn", "schema_mstr_key") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000") \
    .load() \
    .select("schema_name") \
    .cache()



# Iterate over the schema list
for row in schema_list.collect():
    schema_name = row['schema_name']

    # SQL query to create a new database
    query = f"CREATE DATABASE {schema_name};"
    
    execute_query(query)
    
    
    # Example DataFrame to insert
    query = f"""SELECT tm.table_mstr_key, tm.table_name
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
    
    # If you need to join with exclusion_entity, first load it as a DataFrame
    exclusion_df = spark.read \
        .format("jdbc") \
        .option("url", mysql_url) \
        .option("dbtable", f"{mysql_db}.exclusion_rule_master") \
        .option("user", mysql_user) \
        .option("password", mysql_password) \
        .option("driver", mysql_driver) \
        .load()

    # Now perform the join with the DataFrame
    tables_list = tables_list.join(exclusion_df, 
                    (col("table_mstr_key") != col("excluded_entity_mstr_key")) & 
                    (col('exclusion_rule_type_mstr_key') == 1) & 
                    (col('is_active') == 1),
                    how="inner")

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

        
        
        query = f"""WITH exclusion_entity AS (
                    SELECT temp.exclusion_rule_type_mstr_key , group_concat(temp.excluded_entity_mstr_key) as entity_mstr_key FROM (
                        SELECT  erm.exclusion_rule_type_mstr_key ,  erm.excluded_entity_mstr_key FROM mysql_source.exclusion_rule_master erm
                        INNER JOIN mysql_source.exclusion_rule_type_master ertm ON ertm.exclusion_rule_type_mstr_key = erm.exclusion_rule_type_mstr_key AND ertm.is_active = 1
                        LEFT JOIN mysql_source.field_master fm ON fm.field_mstr_key =  erm.excluded_entity_mstr_key AND fm.is_active = 1
                        WHERE erm.is_active = 1
                        UNION
                        SELECT  erm.exclusion_rule_type_mstr_key ,  erm.excluded_entity_mstr_key FROM mysql_source.exclusion_rule_master erm
                        INNER JOIN mysql_source.exclusion_rule_type_master ertm ON ertm.exclusion_rule_type_mstr_key = erm.exclusion_rule_type_mstr_key AND ertm.is_active = 1
                        LEFT JOIN mysql_source.data_type_master dtm ON dtm.data_type_mstr_key =  erm.excluded_entity_mstr_key AND dtm.is_active = 1
                        WHERE erm.is_active = 1)temp
                        GROUP BY temp.exclusion_rule_type_mstr_key)
                    SELECT  CONCAT('CREATE TABLE ', sm.schema_name , '.','dbo','.', tm.table_name ,'('
                    		,GROUP_CONCAT(CONCAT(fm.field_name,' ', IF(dtm2.data_type IS NULL ,dtm.data_type,dtm2.data_type) , IF(fm.max_length IS NOT NULL, CONCAT(' (',  fm.max_length , ') '),''))),
		                    ');') as create_ddl
                    FROM mysql_source.db_type_master dbtm1
                    INNER JOIN mysql_source.schema_master sm ON sm.db_type_mstr_key = dbtm1.db_type_mstr_key AND sm.is_active = 1 
                    INNER JOIN mysql_source.table_master tm ON sm.schema_mstr_key = tm.schema_mstr_key AND tm.is_active = 1
                    INNER JOIN mysql_source.field_master fm ON fm.table_mstr_key = tm.table_mstr_key AND fm.is_active = 1
                    INNER JOIN mysql_source.data_type_master dtm ON dtm.data_type_mstr_key = fm.data_type_mstr_key AND dtm.is_active = 1
                    LEFT JOIN mysql_source.data_type_db_casting_master dtdcm1 ON dtdcm1.source_db_mstr_key = dbtm1.db_type_mstr_key AND dtdcm1.source_data_type_mstr_key = dtm.data_type_mstr_key AND dtdcm1.is_active = 1
                    LEFT JOIN mysql_source.data_type_master dtm2 ON dtm2.data_type_mstr_key = dtdcm1.target_data_type_mstr_key AND dtm2.is_active = 1
                    LEFT JOIN mysql_source.db_type_master dbtm2 ON dbtm2.db_type_mstr_key = dtdcm1.target_db_mstr_key AND dbtm2.is_active =1 AND  dbtm2.db_name = '{target_db_name}'
                    WHERE dbtm1.is_active AND sm.schema_name = '{schema_name}' AND tm.table_name = '{table_name}' AND dbtm1.db_name= '{source_db_name}' 
                    AND dtm.data_type_mstr_key NOT IN (SELECT entity_mstr_key FROM exclusion_entity WHERE exclusion_rule_type_mstr_key = 3)
                    AND dtm2.data_type_mstr_key NOT IN (SELECT entity_mstr_key FROM exclusion_entity WHERE exclusion_rule_type_mstr_key = 3)
                    AND fm.field_name NOT IN (SELECT entity_mstr_key FROM exclusion_entity WHERE exclusion_rule_type_mstr_key = 2)"""
        

        #read data
        query = table_ddl_for_mssql_server = spark.read \
            .format("jdbc") \
            .option("url", mysql_url) \
            .option("query", query) \
            .option("user", mysql_user) \
            .option("password", mysql_password) \
            .option("driver", mysql_driver) \
            .load().select("create_ddl").collect()[0]["create_ddl"]
        
        execute_query(query)
        
        # Write data to SQL Server
        df.write \
            .mode("append") \
            .jdbc(sqlserver_url, f"{schema_name}.dbo.{table_name}", properties=sqlserver_properties) 


# Record the end time
end_time = time.time()

# Calculate the duration
duration = round((end_time - start_time)/60, 2)
print("=============================================")
print(f"Job completed in {duration} minutes.")
print("=============================================")
