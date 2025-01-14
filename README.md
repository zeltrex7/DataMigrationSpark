# Data Migration with Spark

This project demonstrates how to use Apache Spark to migrate data from a MySQL database to a Spark DataFrame and then insert specific data back into a MySQL table.

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

2. Install the required Python packages:
    ```sh
    pip install pyspark python-dotenv
    ```

3. Create a `.env` file in the root directory of the project and add your MySQL credentials:
    ```env
    MYSQL_HOSTNAME=your_mysql_host
    MYSQL_PORT=your_mysql_port
    MYSQL_DATABASE=your_mysql_database
    MYSQL_USERNAME=your_mysql_username
    MYSQL_PASSWORD=your_mysql_password
    MYSQL_DRIVER=com.mysql.cj.jdbc.Driver
    ```

4. Place the MySQL JDBC driver (`mysql-connector-java-8.0.30.jar`) in the appropriate directory (`/opt/spark/apps/jars/`).

5. Configure logging by creating a `log4j.properties` file at `/opt/spark/conf/` with the following content:
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