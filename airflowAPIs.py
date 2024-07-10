# Working
import requests
from requests.auth import HTTPBasicAuth
from graphlib2 import TopologicalSorter        
from airflow.models import BaseOperator
from airflow import DAG
import importlib,sys
from datetime import datetime,timedelta
import re
import os

requests.packages.urllib3.disable_warnings()
 
class GetAirflowTasks:
    
    session = None
    auth = None
    host = None
    host_name = None
    
    def __init__(self):

        self.host = os.environ.get("AIRFLOW_WEBSERVER_URL", 'https://8080-gautami2607-airflowpost-2sm55isbp16.ws-us115.gitpod.io')
        self.username = os.environ.get("AIRFLOW_USERNAME", 'airflow')
        self.password = os.environ.get("AIRFLOW_PASSWORD", 'airflow')

        self.auth = HTTPBasicAuth(self.username, self.password)
        self.session = requests.Session()
        self.session.auth = self.auth

        headers = {
            "Origin": "*",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        self.session.headers.update(headers)
        self.host_name = self.host
        print("In Constructor and host name is: ", self.host_name)
        print('Webserver Host IP ::{}'.format(self.host_name))
        
    def _get_tasks_for_dag(self, dag_id):
        """Get Tasks for a given Airflow DAG
        Args:
            dag_name (str): DAG ID
        Returns:
            string: JSON Object with tasks associated to the DAG
        """
        output = None
        try:
            uri = self.host_name + '/api/v1/dags/'+ dag_id+'/tasks'
            print(uri)
            res = requests.get(uri, auth=self.auth)
            print("res", res)
            output = res.json()
            print("output", output)
            print("Fetched response for _get_tasks_for_dag() %s", output)
        except Exception as e:
            print("Exception while fetching _get_tasks_for_dag() %s for dag_id %s", e, dag_id)
        return output
    
    def get_dags_list(self):
        """
        Get All DAGS List
        Returns:
            string array: List of all DAGS present in airflow
        """
        dags_list = []
        offset = 0
        limit = 100
        try:
            total_count = self.get_dags_count()
            while (offset < total_count):
                res = requests.get(self.host_name + '/api/v1/dags?offset={}'.format(offset), auth=self.auth)
                output = res.json()
                if 'dags' in output:
                    dags = output['dags']
                    for dag in dags:
                        dags_list.append(dag['dag_id'])
                offset = offset + limit
            print('Total DAGS count get_dags_list() = {}'.format(len(dags_list)))   
            print('List is:', dags_list)       
        except Exception as e:
            print("Exception while fetching _get_dags_list() %s", e)
        return dags_list
    
    def get_dags_count(self):
        """
        Get DAGS Count
        Returns:
            int: Count of DAGS on the Airflow Instance
        """
        total_count = 0
        try:
            res = requests.get(self.host_name + '/api/v1/dags', auth=self.auth)
            output = res.json()
            if 'total_entries' in output:
                total_count = output['total_entries']                
            print('Total DAGS count get_dags_count() = {}'.format(total_count))       
        except Exception as e:
            print("Exception while fetching get_dags_count() %s", e)
        return total_count
    
    def get_variables(self):
        """
        Get Airflow Variables
        Returns:
            string array: List of all variables present in airflow
        """
        var_list = []
        try:
            res = requests.get(self.host_name + '/api/v1/variables', auth=self.auth)
            output = res.json()
            if 'variables' in output:
                variables = output['variables']
                for var in variables:
                    var_list.append({var['key'] : var['value']})
            print('Total Variables count = {}'.format(len(var_list)))
        except Exception as e:
            print("Exception while fetching get_variables() %s", e)
        return var_list
    
    def process_tasks_response(self, dag):
        """Get the tasks order from the DAG
        Args:
            dag (str): DAG ID
        Returns:
            list: List of task id's in the sequential order from the dag
        """
        output = self._get_tasks_for_dag(dag_id=dag)
        return output
    
    def get_dag_tasks_list(self, dag):
        """Get the tasks list from the DAG
        Args:
            dag (str): DAG ID
        Returns:
            list: List of task id's in the sequential order from the dag
        """
        
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
        """Get DAG Basic Info
        Args:
            dag_name (str): DAG ID
        Returns:
            string: JSON Object with tasks associated to the DAG
        """
        output = None
        try:
            res = requests.get(self.host_name + '/api/v1/dags/'+dag_id+'', auth = self.auth)
            output = res.json()          
        except Exception as e:
            print("Exception while fetching get_dag_path_info() %s", e)
        return output

    # Private -> Public       
    def _get_dag_details(self, dag_id):
        """Get DAG Basic Info
        Args:
            dag_name (str): DAG ID
        Returns:
            string: JSON Object with tasks associated to the DAG
        """
        output = None
        try:
            res = requests.get(self.host_name + '/api/v1/dags/'+dag_id+'/details', auth = self.auth)
            output = res.json()          
        except Exception as e:
            print("Exception while fetching get_dag_path_info() %s", e)
        return output
        
    # Private -> Public
    def _get_task_names_dict(self, dagId):
        try:
            mod = __import__('dags.'+dagId, fromlist=[''])
            key_list = list(mod.__dict__.keys())
            values_list = list(mod.__dict__.values())
            task_dict = {}
            for k in values_list:
                if(isinstance(k, BaseOperator)):
                    index = values_list.index(k)
                    task_dict[k.task_id] = key_list[index]
            return task_dict
        except Exception as e:
            print("Exception while fetching __get_task_names_dict() %s for dag_id %s", e, dagId)
        return None

    
    def get_dag_file_class_path(self, dag_id):
        final_class_path = None
        try:
            dag_info = self.get_dag_path_info(dag_id=dag_id) 
            dag_file_path = dag_info['fileloc']
            arr = dag_file_path.split(sep='/airflow/', maxsplit=-1)
            final_class_path = arr[-1].replace('.py', '').replace('/', '.')
        
        except Exception as e:
            print("Exception while fetching get_dag_file_class_path() %s for dag_id %s", e, dag_id)
        return final_class_path

    def get_task_info(self, dag_id, task_id):
        output = None
        try:
            res = requests.get(f"{self.host_name}/api/v1/dags/{dag_id}/tasks/{task_id}", auth = self.auth)
            print("res", res)
            output = res.json() 
        except Exception as e:
            print("Exception while fetching get_task_info() %s for dag_id %s", e, task_id)
        return output 

    def get_dagRuns_list(self, dag_id):
        try:
            res = requests.get(f"{self.host_name}/api/v1/dags/{dag_id}/dagRuns", auth = self.auth)
            print("res", res)
            output = res.json() 
        except Exception as e:
            print("Exception while fetching get_task_info() %s for dag_id %s", e, task_id)
        return output
        

    def get_dagRun(self, dag_id, dag_run_id):
        try:
            res = requests.get(f"{self.host_name}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}", auth = self.auth)
            print("res", res)
            output = res.json() 
        except Exception as e:
            print("Exception while fetching get_task_info() %s for dag_id %s", e, task_id)
        return output

    def get_dataset_update(self, dag_id, dag_run_id):
        try:
            res = requests.get(f"{self.host_name}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/upstreamDatasetEvents", auth = self.auth)
            print("res", res)
            output = res.json() 
        except Exception as e:
            print("Exception while fetching get_task_info() %s for dag_id %s", e, task_id)
        return output

    def get_datasets(self):
        try:
            res = requests.get(f"{self.host_name}/api/v1/datasets", auth = self.auth)
            print("res", res)
            output = res.json() 
        except Exception as e:
            print("Exception while fetching get_task_info() %s ", e)
        return output

    def get_datasets_events(self):
        try:
            res = requests.get(f"{self.host_name}/api/v1/datasets/events", auth = self.auth)
            print("res", res)
            output = res.json() 
        except Exception as e:
            print("Exception while fetching get_task_info() %s ", e)
        return output
        


get_airflow = GetAirflowTasks()
get_airflow.get_dags_list()
output = get_airflow.get_datasets()
# output = get_airflow.get_datasets_events()

# output = get_airflow.get_task_info("weekly_time_sheet", "csv_file_available")
# for key in output:
#     print(key, "====", output[key])

# output = get_airflow.get_dagRuns_list("weekly_time_sheet")
# output = get_airflow.get_dagRuns_list("forex_data_pipeline")

# output = get_airflow.get_dagRun("weekly_time_sheet", "scheduled__2024-07-09T00:00:00+00:00")
# output = get_airflow.get_dataset_update("forex_data_pipeline", "scheduled__2024-07-09T00:00:00+00:00")
print(output)