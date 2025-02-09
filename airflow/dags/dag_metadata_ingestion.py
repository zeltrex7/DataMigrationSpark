from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 9),
}

dag = DAG('spark_submit_via_docker', default_args=default_args, schedule_interval=None)

bash_task = BashOperator(
    task_id="submit_spark_job",
    bash_command="""docker exec da-spark-master spark-submit --jars /opt/spark/apps/jars/mysql-connector-java-8.0.30.jar,/opt/spark/apps/jars/mssql-jdbc-12.8.1.jre8.jar \
                --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/apps/log4j.properties" \
                --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/opt/spark/apps/log4j.properties" \
                /opt/spark/apps/scripts/DataIngestion.py""",
    dag=dag

)

