import json
import subprocess
import google.auth
import google.auth.transport.requests
from google.oauth2 import id_token
import requests
import six.moves.urllib.parse

def function_handler(event, context):

    file = event
    bucket_name = file['bucket']
    file_name = file['name']
    process_data = 'gs://' + bucket_name + '/' + file_name
    print('The file is about to be processed: ' + str(process_data))
    data = json.dumps({"conf":{"gs_location": process_data}})
    print('The airflow payload: ' + str(data))


    # Authenticate with Google Cloud.
    # See: https://cloud.google.com/docs/authentication/getting-started
    credentials, _ = google.auth.default(
        scopes=['https://www.googleapis.com/auth/cloud-platform'])
    authed_session = google.auth.transport.requests.AuthorizedSession(
        credentials)

    project_id = 'helical-decoder-322615'
    location = 'us-east1'
    composer_environment = 'airflow'
    dag_name = 'dataproc_job_flow_dag'

    environment_url = (
        'https://composer.googleapis.com/v1beta1/projects/{}/locations/{}'
        '/environments/{}').format(project_id, location, composer_environment)
    composer_response = authed_session.request('GET', environment_url)
    environment_data = composer_response.json()
    airflow_uri = environment_data['config']['airflowUri']

    print(airflow_uri)

    # The Composer environment response does not include the IAP client ID.
    # Make a second, unauthenticated HTTP request to the web server to get the
    # redirect URI.
    redirect_response = requests.get(airflow_uri, allow_redirects=False)
    redirect_location = redirect_response.headers['location']

    print(redirect_response.headers)

    # Extract the client_id query parameter from the redirect.
    parsed = six.moves.urllib.parse.urlparse(redirect_location)
    query_string = six.moves.urllib.parse.parse_qs(parsed.query)
    client_id = query_string['client_id'][0]

    webserver_url = airflow_uri + '/api/experimental/dags/' + dag_name + '/dag_runs'
   
    # Obtain an OpenID Connect (OIDC) token from metadata server or using service account.
    google_open_id_connect_token = id_token.fetch_id_token(google.auth.transport.requests.Request(), client_id)

    print('google_open_id_connect_token:', google_open_id_connect_token)

    # Fetch the Identity-Aware Proxy-protected URL, including an
    # Authorization header containing "Bearer " followed by a
    # Google-issued OpenID Connect token for the service account.   

    returncode = subprocess.run(["curl", "-X", "POST", webserver_url, "-H","Authorization: Bearer {}".format(
        google_open_id_connect_token), "--insecure", "-d", data])
        
    print('returncode:', returncode)

    if returncode==0:
        return {
            'statusCode': 200,
            'body': json.dumps('Function is working!')
        }