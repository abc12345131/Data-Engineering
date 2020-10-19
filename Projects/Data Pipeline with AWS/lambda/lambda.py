import json
import os
import json
import subprocess
def lambda_handler(event, context):
    # TODO implement
    print('dataengineering event handle: ' + str(event))
    records = event['Records'][0]['s3']
    bucket_name = records['bucket']['name']
    file_name = records['object']['key']
    process_data = 's3://' + bucket_name + '/' + file_name
    print('The file is about to be processed: ' + str(process_data))
    endpoint =  os.environ['AIRFLOW_ENDPOINT']
    data = json.dumps({"conf":{"s3_location": process_data}})
    print('The airflow payload: ' + str(data))
    subprocess.run(["curl", "-X", "POST", "{}/api/experimental/dags/EMR_JOB_FLOW_DAG/dag_runs".format(endpoint), "--insecure", "-d", data])
    return {
        'statusCode': 200,
        'body': json.dumps('Lambda is working!')
    }
    