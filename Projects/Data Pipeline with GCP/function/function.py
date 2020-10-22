import json
import subprocess
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests
import six.moves.urllib.parse

IAM_SCOPE = 'https://www.googleapis.com/auth/iam'
OAUTH_TOKEN_URI = 'https://www.googleapis.com/oauth2/v4/token'

def function_handler(event, context):

    file = event
    bucket_name = file['bucket']
    file_name = file['name']
    process_data = 'gs://' + bucket_name + '/' + file_name
    print('The file is about to be processed: ' + str(process_data))
    data = json.dumps({"conf":{"gs_location": process_data}})
    print('The airflow payload: ' + str(data))

    airflow_uri = 'https://b644c9698bf432494p-tp.appspot.com'
    dag_name = 'dataproc_job_flow_dag'
    webserver_url = 'https://' + airflow_uri + '.appspot.com/api/experimental/dags/' + dag_name + '/dag_runs'

    # The Composer environment response does not include the IAP client ID.
    # Make a second, unauthenticated HTTP request to the web server to get the
    # redirect URI.
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers['location']

    # Extract the client_id query parameter from the redirect.
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)
    client_id = query_string['client_id'][0]

    
    # Obtain an OpenID Connect (OIDC) token from metadata server or using service account.
    google_open_id_connect_token = id_token.fetch_id_token(Request(), client_id)

    print('google_open_id_connect_token:', google_open_id_connect_token)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.   
    returncode = subprocess.run(["curl", "-X", "POST", webserver_url, "-H","Authorization: Bearer {}".format(
        google_open_id_connect_token), "--insecure", "-d", data])
    print('returncode:', returncode)

    return {
        'statusCode': 200,
        'body': json.dumps('Function is working!')
    }