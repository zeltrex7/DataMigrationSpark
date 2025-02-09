# Data Migration Project with Apache Spark and Airflow

This project sets up a data migration pipeline using Apache Spark and Apache Airflow in Docker containers.

## Prerequisites

- Docker Desktop
- Git
- 16GB RAM minimum
- Windows 10/11 Pro (for Docker Desktop)

## Project Structure

## Prerequisites

- Python 3.x
- Apache Spark
- MySQL
- `pyspark` library
- `python-dotenv` library

## Setup

1. Clone the repository:
    ```sh
    git clone https://github.com/your-repo/data-migration-spark.git
    cd data-migration-spark
    ```
2. Spin up the containers:
```bash
# Restart all services
docker compose -p spark up -d --scale spark-worker=5
cd airflow
docker compose -p airflow up -d 

```

3. Install the required Python packages:
    ```sh
    pip install pyspark python-dotenv pymssql
    ```

4. Create a `.env` file in the root directory of the project and add your MySQL credentials:
    ```env
    MYSQL_HOSTNAME=your_mysql_host
    MYSQL_PORT=your_mysql_port
    MYSQL_DATABASE=your_mysql_database
    MYSQL_USERNAME=your_mysql_username
    MYSQL_PASSWORD=your_mysql_password
    MYSQL_DRIVER=com.mysql.cj.jdbc.Driver
    ```

5. Place the MySQL JDBC driver (`mysql-connector-java-8.0.30.jar`) in the appropriate directory (`/opt/spark/apps/jars/`).

6. Configure logging by creating a `log4j.properties` file at `/opt/spark/conf/` with the following content:
    ```properties
    # filepath: /opt/spark/conf/log4j.properties
    log4j.rootCategory=ERROR, console
    log4j.appender.console=org.apache.log4j.ConsoleAppender
    log4j.appender.console.target=System.err
    log4j.appender.console.layout=org.apache.log4j.PatternLayout
    log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
    ```

## Running the Script

To run the data ingestion script, execute the following command:
```sh
spark-submit --jars /opt/spark/apps/jars/mysql-connector-java-8.0.30.jar /path/to/DataIngestion.py
```

7. **Verify services**
- Spark Master UI: http://localhost:8081
- Airflow UI: http://localhost:8080
  - Username: admin
  - Password: admin

## Services

- **Spark Master**: Manages Spark cluster
- **Spark Workers**: Execute Spark tasks (5 instances)
- **Airflow**: Orchestrates data pipeline
- **MySQL**: Source database
- **SQL Server**: Target database


## Running Spark Jobs

### Basic Execution

1. **Meta-Data Ingestion**
```bash
spark-submit --jars /opt/spark/apps/jars/mysql-connector-java-8.0.30.jar \
/opt/spark/apps/scripts/MetadataIngestion.py
```

2. **Data Ingestion**
```bash
spark-submit --jars /opt/spark/apps/jars/mysql-connector-java-8.0.30.jar,/opt/spark/apps/jars/mssql-jdbc-12.8.1.jre8.jar \
/opt/spark/apps/scripts/DataIngestion.py
```

### Execution with Error-Only Logging

1. **Meta-Data Ingestion with Log Configuration**
```bash
spark-submit --jars /opt/spark/apps/jars/mysql-connector-java-8.0.30.jar \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/apps/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/apps/log4j.properties" \
/opt/spark/apps/scripts/MetadataIngestion.py
```

2. **Data Ingestion with Log Configuration**
```bash
spark-submit --jars /opt/spark/apps/jars/mysql-connector-java-8.0.30.jar,/opt/spark/apps/jars/mssql-jdbc-12.8.1.jre8.jar \
--conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/apps/log4j.properties" \
--conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/apps/log4j.properties" \
/opt/spark/apps/scripts/DataIngestion.py
```

Note: Ensure all required JAR files are present in the `/opt/spark/apps/jars/` directory and the log4j.properties file is properly configured in `/opt/spark/apps/` before running these commands.