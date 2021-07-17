import os 
import boto3
import re
import datetime
import time
import xml.etree.ElementTree as ET

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

METRIC_GROUP_FILTER = ['cpu','memory']
METRIC_UNIT_MAP = {
    '%':'Percent',
    'KB':'Kilobytes',
}
METRIC_TYPE_CONVERTOR = {
    'float': (lambda val: float(val))
}
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

        temp_file = f"{BASE_DIR}/tmp/{current_ts}_{cluster_id}.tmp"
        os.system(f"ssh -i {SSH_PKEY} -o StrictHostKeyChecking=no -p {SSH_PORT} {SSH_USER}@{private_id} curl http://localhost:8651/?filter=summary > {temp_file}")
        
        doc_ganglia_xml = ET.parse(temp_file).getroot()
        
        for grid in doc_ganglia_xml:
            grid_name = grid.attrib['NAME']
            
            for cluster in grid:
                cluster_name = cluster.attrib['NAME']
                
                print(f"Exporting metrics from {cluster_name} to CloudWatch")
                
                for host in cluster:
                    host_ip = host.attrib['IP']
                    reported = datetime.datetime.fromtimestamp(int(host.attrib['REPORTED'])) 
                    
                    for metric in host:
                        metric_name = metric.attrib['NAME']
                        metric_value = metric.attrib['VAL']
                        metric_type = metric.attrib['TYPE']
                        metric_units = metric.attrib['UNITS']
                        
                        for extra_data in metric:
                            for extra_element in extra_data:
                                
                                if extra_element.attrib['NAME'] == 'GROUP' and extra_element.attrib['VAL'] in METRIC_GROUP_FILTER:
                                    if METRIC_UNIT_MAP.get(metric_units):
                                        cloudwatch_client.put_metric_data(
                                            Namespace='poc-presto',
                                            MetricData=[
                                                {
                                                    'MetricName': f"{metric_name}({metric_units})",
                                                    'Dimensions': [
                                                        {
                                                            'Name': 'host_ip',
                                                            'Value': host_ip
                                                        },
                                                        {
                                                            'Name': 'cluster_name',
                                                            'Value': cluster_name
                                                        },
                                                    ],
                                                    'Timestamp': reported,
                                                    'Value': METRIC_TYPE_CONVERTOR[metric_type](metric_value),
                                                    'Unit': METRIC_UNIT_MAP[metric_units],
                                                },
                                            ]
                                        )
                                        print(f"{metric_name}")
                        
        os.system(f"rm {temp_file}")
