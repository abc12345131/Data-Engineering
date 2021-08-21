# Environment variables
AIRFLOW_ENDPOINT=https://airflow-ec2-ip:8080/
# security group setting

## Make sure inbound and outbound rule is allowing lambda to trigger airflow

## Make sure EC2 role have permission to run EMR

## Run following code in AWS cli to create valid JobFlowRole and ServiceRole
    aws emr create-default-roles
