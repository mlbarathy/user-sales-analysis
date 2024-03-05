from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.email_operator import EmailOperator
from airflow.models import Variable

default_args = {
    'owner': 'DE',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 3),
    'email': Variable.get("developers_email"),
    'email_on_failure': True,   # change to true
    'retries': 0,                # change to 1
    'retry_delay': timedelta(minutes=30),
}

dag = DAG('user_sales_analysis', schedule_interval=None ,catchup=False, default_args=default_args)
ssh_connection_id = Variable.get("EMR_SSH_CONNECTION_ID")

pre = SSHOperator(
        task_id='copy_s3_to_emr',
        ssh_conn_id=ssh_connection_id,
         command='sudo rm -rf /data/workflows/user_sales_analysis && sudo mkdir -p /data/workflows/user_sales_analysis && sudo aws s3 cp s3://{{ var.value.S3_CODE_BUCKET_NAME}}/data/workflows/user_sales_analysis /data/workflows/user_sales_analysis/ --recursive',
        dag=dag)


t1 = SSHOperator(
           task_id='user_sales_analysis'
           ,ssh_conn_id=ssh_connection_id
           ,command="""spark-submit \
           --name User-Sales-Analysis \
           --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
           --class com.sales.controller.UserSalesController.Scala \
           --master {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.masterType}} \
           --queue {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.ExecutionQueue}} \
           --deploy-mode {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.deployMode}} \
           --conf spark.driver.memoryOverhead=512 \
           --conf spark.executor.memoryOverhead=512 \
           --conf spark.dynamicAllocation.maxExecutors={{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.maxExecutors}} \
           --num-executors {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.numExecutors}} \
           --executor-cores {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.executorCores}} \
           --executor-memory {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.executorMemory}} \
           --driver-memory {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.driverMemory}} \
           /data/workflows/user_sales_analysis/spark_emr/target/user-sales-analysis-1.0.jar {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.configFilePath}}  {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.salesDataPath}} {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.apiUserURL}} {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.apiWeatherUrl}} {{var.json.USER_SALES_ANALYSIS_SPARK_CONFIG.apikey}}"""
           ,dag=dag)


pre >> t1