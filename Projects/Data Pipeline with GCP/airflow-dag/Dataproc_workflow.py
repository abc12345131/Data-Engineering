# -*- coding: utf-8 -*-

from datetime import timedelta

import airflow
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitSparkJobOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['wbl@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

CLUSTER_ID = 'j-1RFMC7DZOD6ZX'


dag = DAG(
    'dataproc_job_flow_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval= None
)

def retrieve_s3_file(**kwargs):
    gs_location = kwargs['dag_run'].conf['gs_location'] 
    kwargs['ti'].xcom_push( key = 'gslocation', value = gs_location)

parse_request = PythonOperator(task_id='parse_request',
                             provide_context=True,
                             python_callable=retrieve_gs_file,
                             dag=dag)


SPARK_TEST_STEPS = [
    {
        'Name': 'datajob',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit', 
                '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode','cluster',
                '--num-executors','2',
                '--driver-memory','512m',
                '--executor-memory','3g',
                '--executor-cores','2',
                # 'gs://dataengineering-test/spark-engine_2.11-0.0.1.jar',
                # '-p','Csvparser',
                # '-i','Csv',
                # '-o','parquet',                
                # '-s', "{{ task_instance.xcom_pull('parse_request', key='gslocation') }}", #'-s','gs://dataengineering-test/banking.csv',
                # '-d','s3a://dataengineering-test/results/',
                # '-c','job',
                # '-m','append',
                # '--input-options','header=true'
            ]
        }
    }
]


step_adder = DataprocSubmitSparkJobOperator(
    task_id='add_steps',
    job_flow_id=CLUSTER_ID,
    aws_conn_id='aws_default',
    main_jar='gs://dataengineering-test/spark-engine_2.11-0.0.1.jar',
    arguments=[ 'p=Csvparser',
                'i=Csv','o=parquet',
                's={{ task_instance.xcom_pull("parse_request", key="gslocation") }}',
                'd=gs://dataengineering-test/results/',
                'c=job',
                'm=append',
                'input-options="header=true"']
    dag=dag
)

step_checker = DataprocJobSensor(
    project_id='watch_step',
    location='',
    dataproc_job_id=CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    gcp_conn_id='google_cloud_default', 
    dag=dag
)

step_adder.set_upstream(parse_request)
step_checker.set_upstream(step_adder)
