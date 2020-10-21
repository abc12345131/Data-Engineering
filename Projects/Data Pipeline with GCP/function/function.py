import json
import os
import json
import subprocess
def function_handler(event, context):
    # TODO implement
    file = event
    bucket_name = file['bucket']
    file_name = file['name']
    process_data = 'gs://' + bucket_name + '/' + file_name
    print('The file is about to be processed: ' + str(process_data))
    endpoint =  os.environ['AIRFLOW_ENDPOINT']
    data = json.dumps({"conf":{"gs_location": process_data}})
    print('The airflow payload: ' + str(data))
    subprocess.run(["curl", "-X", "POST", "{}/api/experimental/dags/DATAPROC_JOB_FLOW_DAG/dag_runs".format(endpoint), "--insecure", "-d", data])
    return {
        'statusCode': 200,
        'body': json.dumps('Function is working!')
    }