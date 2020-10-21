# -*- coding: utf-8 -*-

from datetime import timedelta

from airflow import DAG, utils
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['wbl@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

CLUSTER_ID = 'j-1RFMC7DZOD6ZX'


dag = DAG(
    'emr_job_flow_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval= None
)

def retrieve_s3_file(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location'] 
    kwargs['ti'].xcom_push( key = 's3location', value = s3_location)

parse_request = PythonOperator(task_id='parse_request',
                             provide_context=True,
                             python_callable=retrieve_s3_file,
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
                's3://dataengineering-test/wcd_final_project_2.11-0.1.jar',
                '-p','Csvparser',
                '-i','Csv',
                '-o','parquet',                
                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3location') }}", #'-s','s3a://dataengineering-test/banking.csv',
                '-d','s3a://dataengineering-test/results/',
                '-c','job',
                '-m','append',
                '--input-options','header=true'
            ]
        }
    }
]


step_adder = EmrAddStepsOperator(
    task_id='step_adder',
    job_flow_id=CLUSTER_ID,
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=dag
)

step_checker = EmrStepSensor(
    task_id='step_checker',
    job_flow_id=CLUSTER_ID,
    step_id="{{ task_instance.xcom_pull('step_adder', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=dag
)

cluster_terminator = EmrTerminateJobFlowOperator(
    job_flow_id=CLUSTER_ID,
    aws_conn_id='aws_default',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    trigger_rule='one_success',
    dag=dag
)

parse_request >> step_adder >> step_checker >> cluster_terminator >> end
