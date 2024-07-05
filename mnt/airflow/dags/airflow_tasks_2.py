import requests
from requests.auth import HTTPBasicAuth
# from graphlib2 import TopologicalSorter        
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import BaseOperator
from airflow import DAG
import importlib, sys
from datetime import datetime, timedelta
import re
import os

requests.packages.urllib3.disable_warnings()

class GetAirflowTasks:
    
    session = None
    auth = None
    host = None
    host_name = None
    
    # def __init__(self):
    #     self.host = os.environ.get("AIRFLOW_WEBSERVER_URL", 'https://8080-gautami2607-airflowpost-fhlp55tx20r.ws-us115.gitpod.io')
    #     self.username = os.environ.get("AIRFLOW_USERNAME", 'airflow')
    #     self.password = os.environ.get("AIRFLOW_PASSWORD", 'airflow')
        
    #     self.host_name = self.host
    #     print("In Constructor and host name is: ", self.host_name)
    #     LoggingMixin().log.debug('Webserver Host IP ::{}'.format(self.host_name))
    #     self.auth = HTTPBasicAuth(self.username, self.password)
        
    def __init__(self):
        self.host = os.environ.get("AIRFLOW_WEBSERVER_URL", 'https://8080-gautami2607-airflowpost-fhlp55tx20r.ws-us115.gitpod.io')
        self.username = os.environ.get("AIRFLOW_USERNAME", 'airflow')
        self.password = os.environ.get("AIRFLOW_PASSWORD", 'airflow')
        self.host_name = self.host
        self.auth = HTTPBasicAuth(self.username, self.password)

    def handle_request_exception(self, e):
        if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 401:
            LoggingMixin().log.error("Unauthorized access. Check username and password.")
        else:
            LoggingMixin().log.error("Exception occurred: %s", e)

    def get_dags_count(self):
        total_count = 0
        try:
            url_name = "https://8080-gautami2607-airflowpost-fhlp55tx20r.ws-us115.gitpod.io"
            res = requests.get(url_name + '/api/v1/dags', auth=self.auth)
            res.raise_for_status()
            output = res.json()
            if 'total_entries' in output:
                total_count = output['total_entries']                
            LoggingMixin().log.info('Total DAGS count get_dags_count() = {}'.format(total_count))            
        except requests.exceptions.RequestException as e:
            self.handle_request_exception(e)
        return total_count

    def get_dags_list(self):
        dags_list = []
        offset = 0
        limit = 100
        try:
            total_count = self.get_dags_count()
            while offset < total_count:
                res = requests.get(self.host_name + f'/api/v1/dags?offset={offset}&limit={limit}', auth=self.auth)
                res.raise_for_status()
                output = res.json()
                if 'dags' in output:
                    dags = output['dags']
                    for dag in dags:
                        dags_list.append(dag['dag_id'])
                offset = offset + limit
            LoggingMixin().log.info('Total DAGS count get_dags_list() = {}'.format(len(dags_list)))            
        except requests.exceptions.RequestException as e:
            self.handle_request_exception(e)
        return dags_list

    def _get_tasks_for_dag(self, dag_id):
        output = None
        try:
            res = requests.get(self.host_name + f'/api/v1/dags/{dag_id}/tasks', auth=self.auth)
            res.raise_for_status()  # Raises an HTTPError if the HTTP request returned an unsuccessful status code
            output = res.json()
            LoggingMixin().log.info("Fetched response for _get_tasks_for_dag() %s", output)
        except requests.exceptions.RequestException as e:
            LoggingMixin().log.error("Exception while fetching _get_tasks_for_dag() %s for dag_id %s", e, dag_id)
        return output
    
    # def get_dags_list(self):
    #     dags_list = []
    #     offset = 0
    #     limit = 100
    #     try:
    #         total_count = self.get_dags_count()
    #         while offset < total_count:
    #             res = requests.get(self.host_name + f'/api/v1/dags?offset={offset}&limit={limit}', auth=self.auth)
    #             res.raise_for_status()
    #             output = res.json()
    #             if 'dags' in output:
    #                 dags = output['dags']
    #                 for dag in dags:
    #                     dags_list.append(dag['dag_id'])
    #             offset = offset + limit
    #         LoggingMixin().log.info('Total DAGS count get_dags_list() = {}'.format(len(dags_list)))            
    #     except requests.exceptions.RequestException as e:
    #         LoggingMixin().log.error("Exception while fetching get_dags_list() %s", e)
    #     return dags_list
    
    def get_dags_count(self):
        total_count = 0
        try:
            res = requests.get(self.host_name + '/api/v1/dags', auth=self.auth)
            res.raise_for_status()
            output = res.json()
            if 'total_entries' in output:
                total_count = output['total_entries']                
            LoggingMixin().log.info('Total DAGS count get_dags_count() = {}'.format(total_count))            
        except requests.exceptions.RequestException as e:
            LoggingMixin().log.error("Exception while fetching get_dags_count() %s", e)
        return total_count
    
    def get_variables(self):
        var_list = []
        try:
            res = requests.get(self.host_name + '/api/v1/variables', auth=self.auth)
            res.raise_for_status()
            output = res.json()
            if 'variables' in output:
                variables = output['variables']
                for var in variables:
                    var_list.append({var['key']: var['value']})
            LoggingMixin().log.info('Total Variables count = {}'.format(len(var_list)))
        except requests.exceptions.RequestException as e:
            LoggingMixin().log.error("Exception while fetching get_variables() %s", e)
        return var_list
    
    def process_tasks_response(self, dag):
        output = self._get_tasks_for_dag(dag_id=dag)
        return output
    
    def get_dag_tasks_list(self, dag):
        output = self._get_tasks_for_dag(dag)
        dt = {}
        if output:
            for obj in output['tasks']:
                dt[obj['task_id']] = obj['downstream_task_ids']
            
            ts = TopologicalSorter(dt)
            final_dag_order = list(ts.static_order())
            final_dag_order.reverse()
            return final_dag_order
        else:
            return []
        
    def get_dag_path_info(self, dag_id):
        output = None
        try:
            res = requests.get(self.host_name + f'/api/v1/dags/{dag_id}', auth=self.auth)
            res.raise_for_status()
            output = res.json()
        except requests.exceptions.RequestException as e:
            LoggingMixin().log.error("Exception while fetching get_dag_path_info() %s", e)
        return output
       
get_airflow_tasks = GetAirflowTasks()
print(get_airflow_tasks.get_dags_count())
print(get_airflow_tasks.get_dags_list())