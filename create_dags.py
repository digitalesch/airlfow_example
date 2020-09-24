#import airflow
from airflow.models import Variable
from airflow.models import TaskInstance
from airflow import DAG
import airflow

# Operadores utilizados
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.models.baseoperator import BaseOperator
from airflow.contrib.hooks.redshift_hook import RedshiftHook

# Hooks utilizados
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.hooks.gdrive_hook import GoogleDriveHook

#imports adicionais
from datetime import timedelta,datetime

#imports padrões
import os
import json
from typing import List
import yaml
import re
import pandas as pd
import pendulum
from jinja2 import Template
import jinja2
import six

# nome da conexão configurada no airflow, parametros estão abaixo
SLACK_CONN_ID = 'slack_koin'

# função lambda para retornar os parametros incrementais / full para o template
return_string = lambda full,param_full,param_inc: param_full if full > 0 else param_inc
return_params = lambda type,param1,param2: param1 if type == 'db' else param2

'''
    Returns the error message to be sent to Slack by getting the password from the indicated connection

    :param hook_info: dictionary containing generated hook information
    :type hook_info: dict
    :param hook_info: query to be run on the defined hook
    :type hook_info: string
'''
def task_fail_slack_alert(context):
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password
    slack_msg = """
            :red_circle: Task Failed.
            *Task*: {task}
            *Dag*: {dag}
            *Execution Time*: {exec_date}
            *Log Url*: {log_url}
            """.format(
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            ti=context.get('task_instance'),
            exec_date=context.get('execution_date'),
            log_url=(context.get('task_instance').log_url).replace('localhost',Variable.get('server_log_url')),
        )

    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id=SLACK_CONN_ID,
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)

'''
    Returns the query from the connection defined in the argument

    :param hook_info: dictionary containing generated hook information
    :type hook_info: dict
    :param hook_info: query to be run on the defined hook
    :type hook_info: string
'''
def return_records(type,hook,sql_params,dest_type):
    print(type,hook,sql_params,dest_type)
    if type == 'db':
        data = hook.get_pandas_df(sql_params)
        data.columns = map(str.lower, data.columns)
        return data

    return None

'''
    Returns the associated hook for the connection info passed as argument, depending on the needed database

    :param conn_info: dictionary containing generated connection information
    :type conn_info: dict
'''
def return_hook(conn_info):
    if conn_info['hook_type'] == 'postgres':
        return {'type':'db','db_type':conn_info['hook_type'],'hook':PostgresHook(conn_info['conn_id'])}
    if conn_info['hook_type'] == 'redshift':
        return {'type':'db','db_type':conn_info['hook_type'],'hook':RedshiftHook(conn_info['conn_id'])}

    return None

'''
    Returns the query from the connection defined in the argument

    :param hook_info: dictionary containing generated hook information
    :type hook_info: dict
    :param hook_info: query to be run on the defined hook
    :type hook_info: string
'''
def return_where_value(hook_info,query):
    try:
        print('Incremental data: ',hook_info['hook'].get_records(query))
        return hook_info['hook'].get_records(query)
    except:
        return ''

'''
    Returns the truncate statement for the connection defined in the argument

    :param hook_info: dictionary containing generated hook information
    :type hook_info: dict
'''
def return_truncate_command(hook_info):
    if all([hook_info['type'] == 'db',hook_info['db_type'] == 'postgres']):
        return 'truncate '
    if all([hook_info['type'] == 'db',hook_info['db_type'] == 'oracle']):
        return 'truncate table '

    return None

'''
    Transforms the data to a Pandas Dataframe for later insertion through Pandas SQLAlchemy connection

    :param hook_info: dictionary containing generated hook information
    :type hook_info: dict
    :param schema: indicates the schema to be used in the insertion
    :type schema: string
    :param table: indicates the table to be used in the insertion
    :type table: string
    :param records: list of tuples returned by the selection statement from SQLAlchemy
    :type records: list
    :param delete_table: value to be entered if the destination table should be deleted (1) or not (0)
    :type delete_table: int
'''
def insert_values(hook_info,schema,table,records,delete_table=0):
    if delete_table:
        hook_info['hook'].run(return_truncate_command(hook_info)+schema+'.'+table+';')
    (pd.DataFrame(records)).to_sql(name=table,con=hook_info['hook'].get_sqlalchemy_engine(),schema=schema,if_exists='append',index=False,chunksize=10000,method="multi")

'''
    Get leaf task nodes (without ending terminations) in order to be able to create task dependencies

    :param dag: DAG object to create dependencies
    :type dag: DAG
    :param task_names: list indicating task names
    :type task_names: list
'''
def get_leafs_tasks(dag,task_names):
    return [task for task_ids,task in dag.task_dict.items() if len(task.downstream_list)==0 and task_ids not in task_names]

'''
    Retemplates the query, so that the result from the where clause is templated with the value of the result, by querying the destination database

    :param query: Jinja untemplated query
    :type query: string
    :param context: Dict with values to apply on templated content
    :type context: dict
'''
def retemplate_query(query,context):
    return (BaseOperator(task_id='tmp',template_fields=context['params']['untemplated_query'],context=context)).render_template(content=context['params']['untemplated_query'],context=context)

'''
    Returns the query from the connection defined in the argument

    :param hook_info: dictionary containing generated hook information
    :type hook_info: dict
    :param full: value to be entered if the query to be run ignoring the templated fields (1) or not (0)
    :type full: int
    :param context: dictionary with the context of the task
    :type context: dict
'''
def query_definition(hook_info,full,context):
    if full:
        context['params']['where_clause'],context['params']['where_value'] = '',''
    else:
        print((return_where_value(hook_info,context['params']['where_value'])))
        try:
            context['params']['where_value'] = (return_where_value(hook_info,context['params']['where_value']))[0][0]
        except:
            context['params']['where_value'] = ''

    return retemplate_query(context['params']['untemplated_query'],context)

'''
    Reads the records from the defined yaml file

    :param context: dictionary with the context of the task
    :type context: dict
'''
def read_db(**context):
    # source
    #source =
    # dest
    #dest = context['dest']

    # retorna o hook de acordo com a conexão parametrizada
    data_hook = return_hook({'conn_id':context['source']['conn'],'hook_type':context['source']['type'],'add_params':context['source']['add_params']})

    # obtem o hook do destino
    data_hook_dest = return_hook({'conn_id':context['dest']['conn'],'hook_type':context['dest']['type'],'add_params':context['dest']['add_params']})

    new_query = query_definition(data_hook_dest,context['params']['full'],context)

    if context['params']['procedure'] == 'query':
        records = return_records(data_hook['type'],data_hook['hook'],return_params(data_hook['type'],new_query,context['source']['add_params']),data_hook_dest['type'])
        print('Data\n',records)
        insert_values(data_hook_dest,context['dest']['schema'],context['dest']['table'],records,context['params']['delete_table'])
    else:
        data_hook_dest['hook'].run(new_query)

'''
    Create task dependencies configured on the yaml file

    :param dag: DAG object to create dependencies
    :type dag: DAG
    :param task_dependencies: list indicating dependencies between tasks
    :type task_names: list
'''
def create_task_dependencies(dag,task_dependencies):
    for dest_dependency in list(task_dependencies.keys()):
        dest_instance = [dag.get_task(dest_dependency)]
        for source_dependency in task_dependencies[dest_dependency]:
            source_instance = dag.get_task(source_dependency)
            source_instance.set_downstream(dest_instance)

'''
    Parses the config file, in order to evaluate the indicated "eval" fields

    :param kwargs: keyworded arguments that indicate the config file
    :type dag: dict
'''
def parse_eval(**kwargs):
    for key, value in kwargs.items():
        if isinstance(value,dict):
            try:
                value['eval']=eval(value['eval'])
                kwargs[key]=value['eval']
            except:
                kwargs[key] = parse_eval(**value)
    return kwargs

'''
    Create individual dag based on the configuration passed by the parse_eval function

    :param files: list of configuration files ('.yaml') to be used
    :type dag: list
    :param dag_parameters: keyworded arguments that indicate the dag parameters parsed by the parse_eval function
    :type dag: dict
'''
def create_dag(files,**dag_parameters):
    # cria o dag, só preciso de uma configuração senão usa a padrão
    dag = DAG(**dag_parameters)

    # usado para criar as dependencias dos dags
    task_dependencies = {}

    # loop para usar os yamls e criar dags separados
    for file in files:
        task_dependencies=create_dw_transfer_tasks(file,dag,task_dependencies)

    # cria as dependencias
    create_task_dependencies(dag,task_dependencies)

    # retorna o dag criado
    return dag

'''
    Mounts the file and folders parametersCreate individual dag based on the configuration passed by the parse_eval function

    :param files: list of configuration files ('.yaml') to be used
    :type dag: list
    :param dag_parameters: keyworded arguments that indicate the dag parameters parsed by the parse_eval function
    :type dag: dict
'''
def mount_parameters(path,default_args):
    params = {}
    for root, subFolders, files in os.walk(path):
        if len(files)!=0:
            dag_args = {}
            key_name = 'root'+((root.replace(path,''))).replace('/','_')
            try:
                config_file = open(root+'/config','r')
                stream = config_file.read()
                config_file.close()
                dag_args = yaml.safe_load(stream)
            except:
                dag_args = default_args
                dag_args['dag_id'] = key_name

            params[key_name]={'path':root,'files':None}
            params[key_name]['files']=[(file.split('.')[0],root+'/'+file) for file in files if re.match(r'.*.(yaml|yml)',file)]
            params[key_name]['dag_args'] = dag_args
    return params

'''
    Opens the YAML configuration files and sets the respective parameters to create folder / dependencies and DAG structure

    :param file_parameter: tuple with the files information
    :type file_parameter: tuple
    :param dag_name: dag name imported from "config" file
    :type dag: string
    :param task_dependencies: list indicating dependencies between tasks
    :type task_names: list
'''
def create_dw_transfer_tasks(file_parameter,dag_name,task_dependencies):
    stream = open(file_parameter[1])

    # le a primeira linha do sql, onde estão definidos os parametros de origem, destino, tipo de carga (full:0/1) e clausula where para o incremento
    config = yaml.safe_load(stream)

    stream.close()

    # transforma a linha em um json
    json_line = config

    # Pega somente o nome do arquivo, ex: FT_INVOICE.yaml retorna FT_INVOICE
    filename = file_parameter[0]

    # nome das tasks utilizadas
    task_names = {
                    'select':'SELECT_'+filename,
                }

    # task de seleção de dados
    select_task = PythonOperator(
        # campos que serão transformados, conforme o template definido no arquivo sql, carrega o arquivo de transformação
        templates_dict={
                             'query':json_line['query'] #filename+'.sql'
                        },
        task_id=task_names['select'],
        # parametros de origem / destino da conexão
        op_kwargs = {
                        'source':json_line['source'],
                        'dest':json_line['dest'],
                    },
        # parametros de clausula where, definição de task anterior (pode ser obtida através de **context também dentro da função)
        # a clausula where, definida no campo 'where_clause' utiliza a lambda definida no começo para retorno de comentário (carga full) ou da clausula where (incremental)
        params =    {
                    'where_clause':json_line['where']['clause'],
                    'where_value':json_line['where']['value'],
                    'table':json_line['dest']['schema']+'.'+json_line['dest']['table'],
                    'full':json_line['full'],
                    'if_exists':json_line['dest']['add_params']['if_exists'] if 'if_exists' in json_line['dest']['add_params'] else 'append',
                    'delete_table':json_line['dest']['add_params']['delete_table'] if 'delete_table' in json_line['dest']['add_params'] else 0,
                    'procedure':json_line['dest']['add_params']['procedure'] if 'procedure' in json_line['dest']['add_params'] else 'query',
                    'untemplated_query':json_line['query']
                    },
        # função de leitura
        python_callable=read_db,
        trigger_rule='none_failed',
        # manda o contexto da task para ser utilizada
        provide_context = True,
        dag=dag_name,
    )

    conj['SELECT_'+filename]={'flow':[]}
    conj['SELECT_'+filename]['flow'].append(select_task)

    if len(json_line['dag']['dependency']) > 0:
        task_dependencies[task_names['select']] = ['SELECT_'+item for item in json_line['dag']['dependency']]
        conj['SELECT_'+filename]['dependency'] = ['SELECT_'+item for item in json_line['dag']['dependency']]

    return task_dependencies

# Definicoes iniciais do DAG
dag_args = {
                # Intervalo de atualização
                'schedule_interval': '@daily',
                # Mata o DAG caso dê timeout
                'dagrun_timeout':timedelta(minutes=60),
                # Define os argumentos declarados anteriormente
                'default_args':{
                                    'owner': 'airflow',
                                    'start_date': airflow.utils.dates.days_ago(2),
                                    'retries': 0,
                                    'on_failure_callback': 'task_fail_slack_alert'
                                },
                # define a função a ser chamada quando há erro
                'on_failure_callback':task_fail_slack_alert
}

path = Variable.get('config_yaml_path')

params = mount_parameters(path,dag_args)

conj = {}

for item in enumerate(params):
    params[item[1]]['dag_args'] = parse_eval(**params[item[1]]['dag_args'])
    globals()[params[item[1]]['dag_args']['dag_id']] = create_dag(params[item[1]]['files'],**params[item[1]]['dag_args'])
