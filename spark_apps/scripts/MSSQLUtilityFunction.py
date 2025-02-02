from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()
from sqlalchemy.pool import QueuePool

# Configure connection pooling settings
POOL_SIZE = 5  # Number of connections to maintain
MAX_OVERFLOW = 10  # Maximum number of connections that can be created beyond pool_size
POOL_TIMEOUT = 30  # Timeout in seconds for getting a connection from the pool
POOL_RECYCLE = 3600  # Recycle connections after 1 hour

# Get SQL Server credentials from .env
sqlserver_host = os.getenv("SQLSERVER_HOSTNAME")
sqlserver_port = os.getenv("SQLSERVER_PORT")
sqlserver_db = os.getenv("SQLSERVER_DATABASE")

sqlserver_user = os.getenv("SQLSERVER_USERNAME")
sqlserver_password = os.getenv("SQLSERVER_PASSWORD")
sqlserver_driver = os.getenv("SQLSERVER_DRIVER")

def execute_query(query):
    sqlserver_connection_url = f"mssql+pymssql://{sqlserver_user}:{sqlserver_password}@{sqlserver_host}:{sqlserver_port}"
    # Create an SQLAlchemy engine with connection pooling
    engine = create_engine(
        sqlserver_connection_url,
        isolation_level="AUTOCOMMIT",
        poolclass=QueuePool,
        pool_size=POOL_SIZE,
        max_overflow=MAX_OVERFLOW,
        pool_timeout=POOL_TIMEOUT,
        pool_recycle=POOL_RECYCLE
    )
    # SQL query to create a new database
    query = text(query)
    try:
        # Establish connection and execute the query
        with engine.connect() as connection:
            connection.execute(query)

    except Exception as e:
        error_message = str(e).lower()
        
        # Extract object name from the query
        query_str = str(query).lower()
        if "create database" in query_str:
            object_name = query_str.split("database")[-1].split()[0].strip('" ;')
            if "database" in error_message and "already exists" in error_message:
                print(f"Database '{object_name}' already exists")
                return
        elif "create table" in query_str:
            object_name = query_str.split("table")[-1].split()[0].strip('" ;')
            if "there is already an object named" in error_message:
                print(f"Table '{object_name}' already exists")
                return
                
        # For any other errors, show the full error message
        print(f"Query execution failed:\n '{query}' \n:  {e}")
        
        