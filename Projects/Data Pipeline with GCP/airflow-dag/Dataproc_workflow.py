# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
import airflow
from airflow import DAG, utils
from airflow.models import Variable
from airflow.contrib.operators.dataproc_operator import DataprocClusterCreateOperator, \
    DataprocClusterDeleteOperator, DataProcSparkOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.gcp_dataproc_hook import DataProcHook

'''
In the Airflow UI, set variables:
project: GCP project id
region: GCP region ('us-east1')
subnet: VPC subnet id (short id, not the full uri) for me it's default
zone: GCP zone ('us-east1-d')
serviceAccount: if not provided, google default serviceAccount will be used
'''

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['wbl@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10)
}

CLUSTER_NAME = 'dataengineering-test'
JOB_NAME = '{{task.task_id}}-{{ds-nodash}}'

dag = DAG(
    'dataproc_job_flow_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval= None
)

with dag:
    def retrieve_gs_file(**kwargs):
        gs_location = kwargs['dag_run'].conf['gs_location'] 
        kwargs['ti'].xcom_push( key = 'gslocation', value = gs_location)

    parse_request = PythonOperator(
        task_id='parse_request',
        provide_context=True,
        python_callable=retrieve_gs_file
    )


    def ensure_cluster_exists():
        cluster = DataProcHook().get_conn().projects().regions().clusters().get(
            project_Id=Variable.get('project'),
            region=Variable.get('region'),
            clusterName=CLUSTER_NAME
        ).execute(num_retries=3)
        if cluster is None or len(cluster) == 0 or 'clusterName' not in cluster:
            return 'cluster_creater'
        else:
            return 'step_adder'

    cluster_checker = BranchPythonOperator(
        task_id='cluster_checker',
        provide_context=True,
        python_callable=ensure_cluster_exists
    )

    cluster_creater = DataprocClusterCreateOperator(
        task_id='cluster_creater',
        cluster_name=CLUSTER_NAME,
        project_id=Variable.get('project'),
        num_workers=1,
        master_disk_size=50,
        worker_disk_size=50,
        image_version='1.5',
        internal_ip_only=True,
        tags=['dataproc'],
        labels={'dataproc-cluster': CLUSTER_NAME},
        zone=Variable.get('zone'),
        subnetwork_uri='projects/{}/region/{}/subnetworks/{}'.format(
            Variable.get('project'),
            Variable.get('region'),
            Variable.get('subnet')),
        # service_account=Variable.get('serviceAccount')
    )

    step_adder = DataProcSparkOperator(
        task_id='step_adder',
        project_id=Variable.get('project'),
        main_class='Driver.MainApp',
        arguments=[
            '-p','Csvparser',
            '-i','Csv',
            '-o','parquet',                
            '-s', "{{ task_instance.xcom_pull('parse_request', key='gslocation') }}", #'-s','gs://dataengineering-test/banking.csv',
            '-d','gs://dataengineering-test/results/',
            '-c','job',
            '-m','append',
            '--input-options','header=true'
        ],
        job_name=JOB_NAME,
        cluster_name=CLUSTER_NAME,
        dataproc_spark_jars=['gs://dataengineering-test/spark-engine_2.11-0.0.1.jar']
    )

    
    step_checker = DataprocJobSensor(
        task_id='step_checker',
        project_id=Variable.get('project'),        
        location=Variable.get('region'),
        dataproc_job_id="{{ task_instance.xcom_pull(task_ids='step_adder') }}",
        dag=dag
    )

    cluster_terminator = DataprocClusterDeleteOperator(
        task_id='cluster_terminator',
        cluster_name=CLUSTER_NAME,
        project_id=Variable.get('project')
    )

    end = DummyOperator(
        task_id='end',
        trigger_rule='one_success'
    )

    parse_request >> cluster_checker
    cluster_checker >> cluster_creater >> step_adder
    cluster_checker >> step_adder
    step_adder >> step_checker >> cluster_terminator >> end