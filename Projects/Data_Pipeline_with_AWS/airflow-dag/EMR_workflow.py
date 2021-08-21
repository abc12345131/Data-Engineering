# -*- coding: utf-8 -*-

from datetime import timedelta
import airflow
from airflow import DAG, utils
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.hooks.emr_hook import EmrHook


DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['wbl@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

CLUSTER_NAME = 'EMR-test'
CLUSTER_ID = "{{ task_instance.xcom_pull('parse_id', key='clusterid') }}"

JOB_FLOW_OVERRIDES = {
    'Name': CLUSTER_NAME,
    'ReleaseLabel': 'emr-5.29.0',
    "Applications": [ 
        { 
            "Name": "Spark"
        },
            { 
            "Name": "Hadoop"
        },
    ],
    'Instances': {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 2
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole'
}

SPARK_TEST_STEPS = [
    {
        'Name': 'data-engineering-test',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit', 
                '--class', 'Driver.MainApp',
                # '--master', 'yarn',
                # '--deploy-mode','cluster',
                # '--executor-memory','3g',
                # '--num-executors','2',
                's3://data-engineering-project/spark-engine_2.11-0.0.2.jar',
                '-p','Csvparser',
                '-i','Csv',
                '-o','parquet',                
                '-s', "{{ task_instance.xcom_pull('parse_request', key='s3location') }}", #'s3a://data-engineering-project/banking.csv'
                '-d','s3a://data-engineering-test-results-bw/',
                '-c','job',
                '-m','append',
                '--input-options','header=true'
            ]
        }
    }
]


dag = DAG(
    'emr_job_flow_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=1),
    schedule_interval= None
)

with dag:
    def retrieve_s3_file(**kwargs):
        s3_location = kwargs['dag_run'].conf['s3_location'] 
        kwargs['ti'].xcom_push(key = 's3location', value = s3_location)

    parse_request = PythonOperator(
        task_id='parse_request',
        provide_context=True,
        python_callable=retrieve_s3_file
    )

    def ensure_cluster_exists(**kwargs):
        try:
            response = EmrHook().get_conn().list_clusters(ClusterStates=[
                'STARTING', 'RUNNING', 'WAITING'
            ])

            matching_clusters = list(
                filter(lambda cluster: cluster['Name'] == CLUSTER_NAME, response['Clusters'])
            )
            if (len(matching_clusters) >= 1):
                print('cluster is created already!')
                cluster_id = matching_clusters[0]['Id']
                kwargs['ti'].xcom_push(key = 'clusterid', value = cluster_id)
                return 'parse_id'
            else:
                print('cluster does not exist!')
                return 'cluster_creator'

        except Exception as e:
            print('Error:',e)
            return 'cluster_creator'

    cluster_checker = BranchPythonOperator(
        task_id='cluster_checker',
        provide_context=True,
        python_callable=ensure_cluster_exists
    )


    cluster_creator = EmrCreateJobFlowOperator(
        task_id='cluster_creator',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
    )

    def parse_cluster_id(**kwargs):
        cluster = kwargs['ti'].xcom_pull('cluster_checker', key='clusterid')
        nocluster = kwargs['ti'].xcom_pull('cluster_creator', key='return_value')
        if(cluster):
            print('cluster:', cluster)
            kwargs['ti'].xcom_push(key = 'clusterid', value = cluster)
        else:
            print('nocluster:', nocluster)
            kwargs['ti'].xcom_push(key = 'clusterid', value = nocluster)
                
    parse_id = PythonOperator(
        task_id='parse_id',
        provide_context=True,
        python_callable=parse_cluster_id,
        trigger_rule='all_done'
    )


    step_adder = EmrAddStepsOperator(
        task_id='step_adder',
        job_flow_id=CLUSTER_ID,
        aws_conn_id='aws_default',
        steps=SPARK_TEST_STEPS
    )

    step_checker = EmrStepSensor(
        task_id='step_checker',
        job_flow_id=CLUSTER_ID,
        step_id="{{ task_instance.xcom_pull('step_adder', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    cluster_terminator = EmrTerminateJobFlowOperator(
        task_id='cluster_terminator',
        job_flow_id=CLUSTER_ID,
        aws_conn_id='aws_default'
    )

    end = DummyOperator(
        task_id='end'
    )

    parse_request >> cluster_checker
    cluster_checker >> cluster_creator >> parse_id
    cluster_checker >> parse_id
    parse_id >> step_adder >> step_checker >> cluster_terminator >> end