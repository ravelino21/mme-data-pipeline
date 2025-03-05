import json
from google.cloud import storage, secretmanager
import google.auth as gauth
import google.auth.transport.requests as greq
import requests, time, logging, yaml, sys
import pyodbc  # For Cloud SQL connectivity (or use BigQuery client if using BigQuery)
from string import Template
sys.path.append('/opt/airflow/dags/repo/dags')

def get_all_configs(default=True, pipeline_bucket_name='', filepath='', tag=''):
    if default:  
        client = storage.Client()
        bucket = client.bucket(pipeline_bucket_name)
        blob = bucket.blob('config/gcp_config.yml')
        with blob.open('r') as f:
            temp_config = f.read()
            gcp_configs = yaml.load(temp_config, Loader=yaml.FullLoader)

        with open('/opt/airflow/dags/repo/dags/var_config/ms_teams_config.json') as f:
            notif_config = json.load(f)

        with open('/opt/airflow/dags/repo/dags/var_config/dag_config.json') as f:
            dag_config = json.load(f)

        list_configs = gcp_configs['cloud_configuration']['services']
        gcp_main_config = next(item for item in list_configs if item['tag'] == 'main')
        gcp_airflow_config = next(item for item in list_configs if item['tag'] == 'af')
        gcp_dataproc_config = next(item for item in list_configs if item['tag'] == 'gcd')
        return gcp_main_config, notif_config, dag_config, gcp_airflow_config, gcp_dataproc_config

    else:
        target_file = f'/opt/airflow/dags/repo/dags/{filepath}'
        if filepath.split('.')[-1] == 'json':
            with open(target_file) as f:
                temp = json.load(f)
        elif filepath.split('.')[-1] in ['yaml','yml']:
            with open(target_file, 'r') as yf:
                temp = yaml.load(yf, Loader=yaml.FullLoader)
        else:
            return NameError('Format file is unknown!')
        if tag:
            list_configs = temp['cloud_configuration']['services']
            custom_config = next(item for item in list_configs if item['tag'] == tag)
        else:
            custom_config = temp
        return custom_config

    

def get_dataproc_batch_status(batch_config):
    creds, project = gauth.default()
    req = greq.Request()
    creds.refresh(req)
    batch_state, token = 'PENDING', creds.token

    try:
        time.sleep(30.0)
        while batch_state in ['PENDING', 'RUNNING']:
            resp = requests.get(
                    url=f'https://dataproc.googleapis.com/v1/projects/{batch_config["project_id"]}/locations/{batch_config["region"]}/batches/{batch_config["batch_id"]}',
                    headers={'Authorization': 'Bearer ' + token}).text
            batch_state = json.loads(resp)['state']
            if batch_state == 'FAILED':
                logging.error('error message: ' + str(json.loads(resp)['stateMessage']))
                return ValueError
            elif batch_state == 'SUCCEEDED':
                return True
            else:
                time.sleep(30.0)
    except:
        get_dataproc_batch_status(batch_config)

def notif_to_ms_teams(task_status, project_env, dag_owner, notif_config, context):
    title_block = {
        'type': 'TextBlock',
        'size': 'Large',
        'weight': 'Bolder',
        'text': 'Airflow Pipeline v2 notification'
    }
    message_val = f"- **Status**: {task_status}" \
                    f"\r- **Environment**: {project_env}" \
                    f"\r- **Owner**: <at>dagowner</at>" \
                    f"\r- **DAG ID**: {context.get('task_instance').dag_id}" \
                    f"\r- **Task ID**: {context.get('task_instance').task_id}" \
                    f"\r- **Log URL**: {context.get('task_instance').log_url}" \
                    f"\r- **Error**: {context.get('exception')}"
    message_block = {
        'type': 'TextBlock',
        'text': message_val,
        'wrap': 'true'
    }
    notif_config['payload']['attachments'][0]['content']['body'] = [title_block, message_block]
    notif_config['payload']['attachments'][0]['content']['msteams']['entities'][0]['mentioned'] = dag_owner
    requests.post(notif_config['webhook_url'], json=notif_config['payload'], headers=notif_config['headers'])

def get_secrets(secrets_config, project_name, list_of_secrets, return_value, **context):

    client = secretmanager.SecretManagerServiceClient()
    parent = f"projects/{project_name}"
    for secret in client.list_secrets(request={"parent": parent}):
        projectId = secret.name.split("/")[1]
        break

    secret_path = f'projects/{projectId}/secrets/'

    for secr in list_of_secrets:
        temp_val = client.access_secret_version(
            name=secret_path
                +secrets_config['meta']['app_secrets'][secr]['secret_name']
                +'/versions/'
                +str(secrets_config['meta']['app_secrets'][secr]['secret_version'])
        ).payload.data.decode('UTF-8')
        if return_value:
            return temp_val
        else:
            context['ti'].xcom_push(key=f'{secr}_secret_value', value=temp_val)




def sql_template(sql_filepath, subtitution_obj=dict()):
    with open(sql_filepath, 'r') as fp:
        sql = fp.read()
    return Template(sql).substitute(subtitution_obj)
