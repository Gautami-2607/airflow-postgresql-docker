import requests
from requests.auth import HTTPBasicAuth
from graphlib2 import TopologicalSorter        
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import BaseOperator
# from bmc.util.env_util import EnvironmentUtil
from airflow import DAG
import importlib,sys
from datetime import datetime,timedelta
import re
import os

requests.packages.urllib3.disable_warnings()

XCOM_PUSH_PULL_REGEX = r"(xcom_pu..)(\(.*?\))"
ENV_VAR_REGEX = r"os.environ.get\((.*?)\)"   
 
class GetAirflowTasks:
    
    session = None
    auth = None
    host = None
    host_name = None
    
    def __init__(self):

        # self.host = EnvironmentUtil.get_env_value("AIRFLOW_WEBSERVER_URL", 'http://host.docker.internal:8080')
        self.host = os.environ.get("AIRFLOW_WEBSERVER_URL", 'https://8080-gautami2607-airflowpost-fhlp55tx20r.ws-us115.gitpod.io')
        self.username = os.environ.get("AIRFLOW_USERNAME", 'airflow')
        self.password = os.environ.get("AIRFLOW_PASSWORD", 'airflow')

        # self.username = 'airflow'
        # self.password = 'airflow'
            
        # GP_WS = EnvironmentUtil.get_env_value('GITPOD_WORKSPACE_URL', None)
        # if GP_WS:
        #     self.host = GP_WS.replace('https://', 'https://8080-')
        
        self.host_name = self.host
        print("In Constructor and host name is: ", self.host_name)
        LoggingMixin().log.debug('Webserver Host IP ::{}'.format(self.host_name))
        self.auth = HTTPBasicAuth('airflow', 'airflow')
        
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
            #LoggingMixin().log.info("Fetched response for _get_tasks_for_dag() %s", output)
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching _get_tasks_for_dag() %s for dag_id %s", e, dag_id)
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
                url_name = "https://8080-gautami2607-airflowpost-fhlp55tx20r.ws-us115.gitpod.io"
                # res = requests.get(self.host_name + '/api/v1/dags?offset={}'.format(offset), auth=self.auth)
                res = requests.get(url_name + '/api/v1/dags?offset={}'.format(offset), auth=self.auth)
                output = res.json()
                if 'dags' in output:
                    dags = output['dags']
                    for dag in dags:
                        dags_list.append(dag['dag_id'])
                offset = offset + limit
            LoggingMixin().log.info('Total DAGS count get_dags_list() = {}'.format(len(dags_list)))            
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching _get_dags_list() %s", e)
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
            LoggingMixin().log.info('Total DAGS count get_dags_count() = {}'.format(total_count))            
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_dags_count() %s", e)
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
            #LoggingMixin().log.info('Total Variables count = {}'.format(len(var_list)))
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_variables() %s", e)
        return var_list
    
    def process_tasks_response(self, dag):
        """Get the tasks order from the DAG
        Args:
            dag (str): DAG ID
        Returns:
            list: List of task id's in the sequential order from the dag
        """
        
        output = self._get_tasks_for_dag(dag_id=dag)
        # dt = {}
        # taskop_dict = {}
        # for obj in output['tasks']:
        #     taskop_dict[obj['task_id']] = obj['class_ref']['class_name']
        #     dt[obj['task_id']] = obj['downstream_task_ids']
        
        # ts = TopologicalSorter(dt)
        # final_dag_order = list(ts.static_order())
        # final_dag_order.reverse()
        # return (final_dag_order, taskop_dict)
        return output
    
    # My comment
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
            LoggingMixin().log.error("Exception while fetching get_dag_path_info() %s", e)
        return output
            
    def __get_dag_details(self, dag_id):
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
            LoggingMixin().log.error("Exception while fetching get_dag_path_info() %s", e)
        return output

    def get_dag_source_code(self, dag_id, file_token):
        """Get DAG Source Code
        Args:
            dag_name (str): DAG ID
            file_token (str): File Token
        Returns:
            string: JSON Object with tasks associated to the DAG
        """
        output = None
        try:
            res = requests.get(self.host_name + '/api/v1/dagSources/' +file_token, auth = self.auth)
            output = res.text            
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching _get_dag_source_code() %s for dag_id=%s", e, dag_id)
        return output   
    
    
    def get_xcom_and_env_vars(self, dag_id):
        dag_info = self.get_dag_path_info(dag_id=dag_id)
        source_code = self.get_dag_source_code(dag_id, dag_info['file_token'])
        
        xcom_env_dict = {}
        xcom_pull_list = []
        xcom_push_list = []
        has_hook = False
        
        if '.hooks.' in source_code:
            has_hook = True
        
        xcom_env_dict['hooks'] = has_hook
        
        matches = re.finditer(XCOM_PUSH_PULL_REGEX, source_code, re.MULTILINE)

        for matchNum, match in enumerate(matches, start=1):
            match_grp = match.group()
            group2 = match.group(2)
            group2 = group2.replace('(', '').replace(')', '')
            group2_keys = group2.split(",")
            xcom_obj = {}
            for obj in group2_keys:
                for key in ['key', 'task_ids', 'value']:
                    if "{}=".format(key) in obj:
                        xcom_obj[key] = str(obj.strip().replace('{}='.format(key), '').replace("\"", '').replace("'", ''))
                if xcom_obj == {}:
                    xcom_obj['key'] = str(obj.strip().replace('{}='.format(key), '').replace("\"", '').replace("'", ''))
            # if 'value' not in xcom_obj and 'key' in xcom_obj:
            #     xcom_pull_list.append(xcom_obj['key'])
            # else:
            #     xcom_push_list.append(xcom_obj['key'])
                
        xcom_env_dict['xcom_pull'] =  xcom_pull_list      
        xcom_env_dict['xcom_push'] =  xcom_push_list
        
        env_var_list = []
        matches = re.findall(ENV_VAR_REGEX, source_code, re.MULTILINE)
        for obj in matches:
            obj = str(obj.strip().replace("\"", '').replace("'", ''))
            obj_tuple = tuple(obj.split(','))
            env_var_list.append(obj_tuple[0])
        xcom_env_dict['env_vars'] =  env_var_list   
        
        
        return xcom_env_dict
        
    def __get_attribute(self, dagId, variable):
        try:
            #print(variable)
            mod = __import__('dags.'+dagId, fromlist=[variable])
            att = getattr(mod, variable)
            return att
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching __get_attribute() %s", e)
        return None

    def __get_task_names_dict(self, dagId):
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
            LoggingMixin().log.error("Exception while fetching __get_task_names_dict() %s for dag_id %s", e, dagId)
        return None
        
    def get_commands(self, dag_id):
        tasks_dict = {}
        try:
            tasks_list = self.get_dag_tasks_list(dag_id)
            tasks_dict = self.__get_task_names_dict(dag_id)
            for task_id in tasks_list:
                task_name = tasks_dict[task_id]
                try:
                    att = self.__get_attribute(dag_id, task_name)
                    tasks_dict[task_id] = att
                    # if(isinstance(att, BashOperator)):
                    #     task_cmd = att.bash_command
                    # elif(isinstance(att, BranchPythonOperator) or isinstance(att, PythonOperator)):
                    #     task_cmd = att.python_callable
                    # tasks_dict[task_id] = task_cmd#task_cmd#{ 'cmd': task_cmd, 'do_xcom_push': 'False' if 'do_xcom_push' not in att else att.do_xcom_push}
                except Exception as e:
                    LoggingMixin().log.info("Could not find bash_command for the task :: {} is {}".format(task_id, e))
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_commands() %s", e)
        return tasks_dict
    
    def get_commands_using_exec(self, dag_id):
        tasks_dict = {}
        try:
            dag_info = self.get_dag_path_info(dag_id=dag_id)
            
            source_code = self.get_dag_source_code(dag_id, dag_info['file_token'])
            # global dag
            # exec(source_code, globals())
            dag = None
            name = 'dag_module'
            spec = importlib.util.spec_from_loader(name, loader=None)
            module = importlib.util.module_from_spec(spec)
            
            # exec() function is used for the dynamic execution of Python program which can either be a string or object code
            exec(source_code, module.__dict__)
            sys.modules[name] = module
            globals()[name] = module
            
            values_list = list(module.__dict__.values())
            for k in values_list:
                if isinstance(k, DAG):
                    dag = k
                    if dag_id == dag.dag_id:
                        # for task in k.tasks:
                        #     print(task.template_fields)
                        for task in dag.tasks:
                            try:
                                task_id = task.task_id
                                tasks_dict[task_id] = task
                            except Exception as e:
                                LoggingMixin().log.info("Exception while assigning the tasks_dict :: {} is {}".format(task_id, e))
                    
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_commands_using_exec() %s", e)
        return tasks_dict
    
    # def get_commands_using_exec(self, dag_id):
    #     dag_tasks_dict = {}
    #     try:
    #         dag_info = self.get_dag_path_info(dag_id=dag_id)
            
    #         source_code = self.__get_dag_source_code(dag_info['file_token'])
    #         # global dag
    #         # exec(source_code, globals())
    #         dag = None
    #         name = 'dag_module'
    #         spec = importlib.util.spec_from_loader(name, loader=None)
    #         module = importlib.util.module_from_spec(spec)
            
    #         # exec() function is used for the dynamic execution of Python program which can either be a string or object code
    #         exec(source_code, module.__dict__)
    #         sys.modules[name] = module
    #         globals()[name] = module
            
    #         values_list = list(module.__dict__.values())
    #         for k in values_list:
    #             if isinstance(k, DAG):
    #                 dag = k
    #                 # for task in k.tasks:
    #                 #     print(task.template_fields)
    #                 tasks_dict = {}
    #                 for task in dag.tasks:
    #                     try:
    #                         task_id = task.task_id
    #                         tasks_dict[task_id] = task
    #                     except Exception as e:
    #                         LoggingMixin().log.info("Exception while assigning the tasks_dict :: {} is {}".format(task_id, e))
                    
    #                 dag_tasks_dict[dag_id] = tasks_dict
    #     except Exception as e:
    #         LoggingMixin().log.error("Exception while fetching get_commands_using_exec() %s", e)
    #     return dag_tasks_dict

    
    def get_dag_file_class_path(self, dag_id):
        
        final_class_path = None
        try:
            dag_info = self.get_dag_path_info(dag_id=dag_id)
                
            dag_file_path = dag_info['fileloc']
                
            arr = dag_file_path.split(sep='/airflow/', maxsplit=-1)
            final_class_path = arr[-1].replace('.py', '').replace('/', '.')
        
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_dag_file_class_path() %s for dag_id %s", e, dag_id)
        return final_class_path
    
    
    def get_all_connections(self):
        conn_list = []
        try:
            res = requests.get(self.host_name + '/api/v1/connections', auth=self.auth)
            output = res.json()
            if 'connections' in output:
                conn_list = output['connections']
                
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_all_connections() %s ", e)
        return conn_list
    
    def get_connection(self, conn_id):
        output = None
        try:
            res = requests.get(self.host_name + '/api/v1/connections/'+conn_id, auth=self.auth)
            if res:
                output = res.json()
            #LoggingMixin().log.info("Fetched response for _get_tasks_for_dag() %s", output)
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_connection() %s for conn_id %s", e, conn_id)
        return output

    def get_schedule(self,dag_id):
        try:
            dag_info:dict = self.__get_dag_details(dag_id=dag_id)
            schedule_interval_dict:dict = dag_info.get("schedule_interval",None)
            schedule_interval=schedule_interval_dict.get("value",None) if schedule_interval_dict else None
            start_date = dag_info.get("start_date",None)
            if start_date:
                start_date_control_m = datetime.fromisoformat(start_date).strftime("%Y%m%d")
            else:
                start_date_control_m = None
            end_date = dag_info.get("end_date",None)
            if end_date:
                end_date_control_m = datetime.fromisoformat(end_date).strftime("%Y%m%d")
            else:
                end_date_control_m = (datetime.fromisoformat(start_date) + timedelta(days=4)).strftime("%Y%m%d") if start_date else None
            
            return schedule_interval,start_date_control_m,end_date_control_m
        except Exception as e:
            LoggingMixin().log.error("Exception while fetching get_schedule() %s for dag_id %s", e, dag_id)

# container = docker.DockerClient().containers.get("airflow_airflow-webserver_1")
# print(container.attrs['NetworkSettings']['Networks']['airflow-network']['IPAddress'])
# ip_add = container.attrs['NetworkSettings']['IPAddress']
# print(ip_add)

#print(GetAirflowTasks().get_xcom_and_env_vars('complex_airflow_s3_ops_etl'))

get_airflow_tasks = GetAirflowTasks()
get_airflow_tasks.get_dags_list()
get_airflow_tasks.get_all_connections()
print('get_airflow_tasks._get_tasks_for_dag("forex_data_pipeline")')
get_airflow_tasks._get_tasks_for_dag("forex_data_pipeline")