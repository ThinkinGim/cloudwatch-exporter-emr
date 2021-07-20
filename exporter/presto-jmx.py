import os
import subprocess
import boto3
import re
import datetime
import time
import csv

from configparser import ConfigParser

BASE_DIR = os.getcwd()
EMR_CONFIG = f"{BASE_DIR}/emr.conf"

if not os.path.isfile(EMR_CONFIG):
    print(f"{EMR_CONFIG} does not exist")
    exit()

config = ConfigParser()
config.read(f"{BASE_DIR}/emr.conf")

SSH_USER = config['general']['ssh_user']
SSH_PORT = config['general']['ssh_port']
SSH_PKEY = config['general']['ssh_pkey']
if not os.path.isfile(SSH_PKEY):
    print(f"{SSH_PKEY} does not exist")
    exit()

client = boto3.client('emr')
cloudwatch_client = boto3.client('cloudwatch')

res_clusters = client.list_clusters(
    ClusterStates=['RUNNING', 'WAITING']
)

current_ts = int(time.time())

for cluster in res_clusters['Clusters']:
    cluster_id = cluster['Id']
    
    res_instances = client.list_instances(
        ClusterId=cluster_id,
        InstanceGroupTypes=['MASTER'],
        InstanceStates=['RUNNING'],
    )

    for instance in res_instances['Instances']:
        private_id = instance['PrivateIpAddress']

        TEMP_FILE = f"{BASE_DIR}/tmp/{current_ts}_presto_{cluster_id}.tmp"
        CMD_SSH = f"ssh -i {SSH_PKEY} -o StrictHostKeyChecking=no -p {SSH_PORT} {SSH_USER}@{private_id} "
        PRESTO_CMD = f"'presto-cli --catalog jmx --schema jmx --output-format CSV --execute \"select queuedqueries, runningqueries from jmx.current.\\\"com.facebook.presto.execution:name=querymanager\\\"\"'"
        
        # print(f"{CMD_SSH}{PRESTO_CMD}")
        result = subprocess.check_output(f"{CMD_SSH}{PRESTO_CMD}", shell=True).decode('utf-8')
        results = result.split('\n')[0].split(',')

        cloudwatch_client.put_metric_data(
            Namespace='poc-presto',
            MetricData=[
                {
                    'MetricName': "queuedqueries",
                    'Dimensions': [
                        {
                            'Name': 'cluster_name',
                            'Value': cluster_id
                        },
                    ],
                    'Timestamp': current_ts,
                    'Value': float(results[0].replace('"','')),
                    'Unit': 'None',
                },
            ]
        )
        
        cloudwatch_client.put_metric_data(
            Namespace='poc-presto',
            MetricData=[
                {
                    'MetricName': "runningqueries",
                    'Dimensions': [
                        {
                            'Name': 'cluster_name',
                            'Value': cluster_id
                        },
                    ],
                    'Timestamp': current_ts,
                    'Value': float(results[1].replace('"','')),
                    'Unit': 'None',
                },
            ]
        )