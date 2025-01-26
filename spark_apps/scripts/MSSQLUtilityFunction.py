from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

# Get SQL Server credentials from .env
sqlserver_host = os.getenv("SQLSERVER_HOSTNAME")
sqlserver_port = os.getenv("SQLSERVER_PORT")
sqlserver_db = os.getenv("SQLSERVER_DATABASE")
sqlserver_user = os.getenv("SQLSERVER_USERNAME")
sqlserver_password = os.getenv("SQLSERVER_PASSWORD")
sqlserver_driver = os.getenv("SQLSERVER_DRIVER")


def execute_query(query):
    sqlserver_connection_url = f"mssql+pymssql://{sqlserver_user}:{sqlserver_password}@{sqlserver_host}:{sqlserver_port}"
    # Create an SQLAlchemy engine
    engine = create_engine(sqlserver_connection_url, isolation_level="AUTOCOMMIT")
    # SQL query to create a new database
    query = text(query)
    try:
        # Establish connection and execute the query
        with engine.connect() as connection:
            connection.execute(query)
            print("Query executed successfully.")

    except Exception as e:
        print(f"Query execution failed :\n '{query}' \n:  {e}")