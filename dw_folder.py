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
from airflow.operators.python_operator import BranchPythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.contrib.hooks.redshift_hook import RedshiftHook

# Hooks utilizados
from airflow.gcp.hooks.gsheets import GSheetsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.base_hook import BaseHook
from airflow.contrib.hooks.gdrive_hook import GoogleDriveHook

#imports adicionais
from datetime import timedelta
from psycopg2.extras import execute_values
import cx_Oracle

#imports padrões
import os
import json
from typing import List
import yaml
import re
import pandas as pd

# nome da conexão configurada no airflow, parametros estão abaixo
SLACK_CONN_ID = 'slack'

# função lambda para retornar os parametros incrementais / full para o template
return_string = lambda full,param_full,param_inc: param_full if full > 0 else param_inc
return_params = lambda type,param1,param2: param1 if type == 'db' else param2

# task de alerta de erro no slack, necessário criar a conexão slack com os parâmetros 
# criado um webhook de teste na minha conta do slack
# conn id: slack
# conn type: http
# host: https://hooks.slack.com/services
# password: /T13PED09H/BQB6TUKB3/oCPuNxa9cFj0110j2KCBnfPj -- obtido através da url do aplicativo do slack
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
            log_url=context.get('task_instance').log_url,
        )

    # task gerada para contemplar a função de envio de mensagens
    failed_alert = SlackWebhookOperator(
        task_id='slack_test',
        http_conn_id='slack',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        username='airflow')
    return failed_alert.execute(context=context)

# função para pegar os registros
def return_records(type,hook,sql_params,dest_type):
    print(type,hook,sql_params,dest_type)
    if type == 'db':
        if dest_type == 'flat_file':
            return hook.get_pandas_df(sql_params)
        else:
            data = hook.get_pandas_df(sql_params)
            data.columns = map(str.lower, data.columns)
            return data
    elif type == 'gsheets':
        return hook.get_values(range_=sql_params['range'],value_render_option=sql_params['value_render_option'])    

# retorna o tipo de conexão necessária para identificar a origem do dado
# conn_info é um dicionario com o nome ("conn_id"), tipo "hook_type" e parametros adicionais
def return_hook(conn_info):
    if conn_info['hook_type'] == 'postgres':
        return {'type':'db','db_type':conn_info['hook_type'],'hook':PostgresHook(conn_info['conn_id'])}
    if conn_info['hook_type'] == 'redshift':
        return {'type':'db','db_type':conn_info['hook_type'],'hook':RedshiftHook(conn_info['conn_id'])}
    if conn_info['hook_type'] == 'oracle':
        return {'type':'db','db_type':conn_info['hook_type'],'hook':OracleHook(conn_info['conn_id'])}
    if conn_info['hook_type'] =='gsheets':
        return {'type':'gsheets','db_type':'','hook':GSheetsHook(gcp_conn_id = conn_info['conn_id'],spreadsheet_id = conn_info['add_params']['spreadsheet_id'],api_version = conn_info['add_params']['api_version'])}    
    if conn_info['hook_type'] in ['csv','xlsx']:
        return {'type':'flat_file','db_type':conn_info['hook_type'] or 'csv','hook':conn_info['add_params']['path']+conn_info['add_params']['file_name']+'.'+conn_info['hook_type'] if len(conn_info['hook_type'])>0 else 'csv'}
    if conn_info['hook_type'] == 'drive':
        return {'type':'gdrive','db_type':conn_info['hook_type'],'hook':GoogleDriveHook(gcp_conn_id = conn_info['conn_id'], api_version = conn_info['add_params']['version'])}

    return None

def return_truncate_command(hook):
    if hook['type']=='db':
        if hook['db_type']=='postgres':
            return 'truncate '
        if hook['db_type']=='oracle':
            return 'delete '
    return None

# transformar tudo pra dataframe, pra depois inserir pela conexão
def insert_values(conn_info,dest_connection,records,if_exists='append',add_params=None):    
    print(conn_info)
    print(dest_connection)
    if conn_info['type']=='flat_file':
        if conn_info['db_type'] == 'csv':
            pd.DataFrame.to_csv(records,path_or_buf=conn_info['hook'],index=False,sep=';')
        elif conn_info['db_type']=='xlsx':
            pd.DataFrame.to_excel(records,excel_writer=conn_info['hook'],index=False)
    else:
        (pd.DataFrame(records)).to_sql(name=conn_info['table'],con=dest_connection,schema=conn_info['schema'],index=False,chunksize=10000,if_exists=if_exists,method="multi")


def get_leafs_tasks(dag,task_names):
    return [task for task_ids,task in dag.task_dict.items() if len(task.downstream_list)==0 and task_ids not in task_names]

def return_branch(**context):
    if context['params']['full'] == 1 or len(context['params']['where_clause']) == 0:
        context['ti'].xcom_push(key='return_value', value=[(context.get('task_instance').task_id,0)])
        return context['params']['branch_select']
    else:
        context['ti'].xcom_push(key='return_value', value=[(context['params']['branch_where'],0)])
        return context['params']['branch_where']

# função que retorna o where full / incremental para a query
def where_db(**context):
    print(context['params']['full'])
    if context['params']['full'] == 1:
        # retorna a string de comentário do sql, para ser utilizada quando houver o template dos arquivos sqls na carga full
        return '------------'
    else:        
        data_hook = return_hook({'conn_id':context.get('source')['conn'],'hook_type':context.get('source')['type']})
        return data_hook['hook'].get_records(context['params']['query'])

# função que lê os registros conforme os parâmetros utilizados
def read_db(**context):    
    # source
    source = context.get('source')
    print(source)
    
    # dest
    dest = context.get('dest')
    print(dest)

    # retorna o hook de acordo com a conexão parametrizada
    data_hook = return_hook({'conn_id':source['conn'],'hook_type':source['type'],'add_params':source['add_params']})

    # obtem o hook do destino
    data_hook_dest = return_hook({'conn_id':dest['conn'],'hook_type':dest['type'],'add_params':dest['add_params']})

    print(data_hook_dest)

    # colocado para testar comandos isolados, tipo execução de procedures
    if context['params']['procedure'] == 'query':
        # printa no log a query já utilizando o template
        print(context['templates_dict']['query'])

        # Abre o arquivo passado como parametro e realiza a query indicada
        records = return_records(data_hook['type'],data_hook['hook'],return_params(data_hook['type'],context['templates_dict']['query'],source['add_params']),data_hook_dest['type'])
        
        print(records)

        # coloca em tuplas, visto que o cursor precisa desse tipo dados para rodar o insert
        if data_hook['type'] == 'gsheets':
            results = []
            for item in records['values']:
                results.append(item)
            records = pd.DataFrame(results)
        
        # cursor para utilizar escrita dos dados
        if data_hook_dest['type'] in ['db','gsheets']:
            # pega a engine do sqlalchemy
            dest_conn = data_hook_dest['hook'].get_sqlalchemy_engine()
            
            # caso seja full, deleta os dados (se houver)    
            if context['params']['full'] == 1:
                print(return_truncate_command(data_hook_dest))
                print(return_truncate_command(data_hook_dest)+context['params']['table']+';')                
                if context['params']['if_exists'] not in ['replace']:
                    data_hook_dest['hook'].run(return_truncate_command(data_hook_dest)+context['params']['table']+';')
                
            # insere os valores
            insert_values(dest,dest_conn,records,context['params']['if_exists'],context['params']['upsert'])
            
            data_hook = None
            data_hook_dest = None
            dest_conn = None
        elif data_hook_dest['type'] in ['flat_file']:
            print(data_hook_dest['hook'])
            # retira a primeira coluna que contêm o índice        
            results = records.drop(records.columns[1], axis=1)
            insert_values(data_hook_dest,None,results)            
    else:
        data_hook_dest['hook'].run(context['templates_dict']['query'])

# cria as interdependências de tasks, para que seja possível gerar stages e depois consumi-las
def create_task_dependencies(dag,task_dependencies,end_task):
    print(task_dependencies)
    for dest_dependency in list(task_dependencies.keys()):    
        dest_instance = [dag.get_task(dest_dependency)]
        for source_dependency in task_dependencies[dest_dependency]:
            source_instance = dag.get_task(source_dependency)        
            source_instance.set_downstream(dest_instance)
    
    get_leafs_tasks(dag,['END']) >> end_task

def create_dag(dag_id,schedule,default_args,item,params):
    # cria o dag, só preciso de uma configuração senão usa a padrão
    print(dag_id)
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)
   
    # dummy start
    start = DummyOperator(
            task_id='START',
            dag=dag
        )

    # dummy end
    end = DummyOperator(
            task_id = 'END',
            trigger_rule='none_failed',
            dag=dag
        )
    
    # usado para criar as dependencias dos dags
    task_dependencies = {}

    # loop para usar os yamls e criar dags separados
    for file in params[item]['files']:
        task_dependencies=create_dw_transfer_tasks(file,dag,task_dependencies) 
            
    # cria as dependencias
    create_task_dependencies(dag,task_dependencies,end)

    # retorna o dag criado
    return dag

def mount_parameters(path):
    params = {}    
    for root, subFolders, files in os.walk(path):
        if len(files)!=0:
            key_name = 'root'+((root.replace(path,''))).replace('/','_')
            params[key_name]={'path':root,'files':None}
            params[key_name]['files']=[(file.split('.')[0],root+'/'+file) for file in files if re.match(r'.*.(yaml|yml)',file)]            
    return params

def create_dw_transfer_tasks(file_parameter,dag_name,task_dependencies):
    stream = open(file_parameter[1])
    
    # le a primeira linha do sql, onde estão definidos os parametros de origem, destino, tipo de carga (full:0/1) e clausula where para o incremento
    config = yaml.safe_load(stream)

    # transforma a linha em um json
    json_line = config
    
    # Pega somente o nome do arquivo, ex: FT_INVOICE.yaml retorna FT_INVOICE
    filename = file_parameter[0]

    # nome das tasks utilizadas
    task_names = {
                    'branch':'BRANCH_'+filename,
                    'where':'WHERE_'+filename,
                    'select':'SELECT_'+filename,
                }

    branch_task = BranchPythonOperator(
            task_id=task_names['branch'],
            params = {
                    'full':json_line['full'],
                    'where_clause':json_line['where']['clause'],
                    'branch_where':task_names['where'],
                    'branch_select':task_names['select']
            },            
            provide_context=True,
            python_callable=return_branch,
            trigger_rule='none_failed',
            dag=dag_name,
        )

    # definição da task where, sendo que:
    # incremental: utiliza clausula where definida, caso contrário, é retornado comentário sql ('-----') para o template
    where_task = PythonOperator(
        # nome da task como definido acima
        task_id=task_names['where'],
        # definição de parametros de origem dos dados para a clausula where
        op_kwargs = {
                        'source':json_line['dest'],                        
                    },
        # parametros utilizados nas funções
        params = {
                    'query':json_line['where']['value'],
                    'full':json_line['full']
        },
        # função a ser chamada pela task
        python_callable=where_db,
        # passa o contexto da task para ser chamada
        provide_context = True,      
        # definição da dag  
        dag=dag_name
    )

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
                    'where_clause':return_string(json_line['full'],'------------',json_line['where']['clause']),
                    'prev_task':return_string(json_line['full'],task_names['branch'],task_names['where']),
                    'table':json_line['dest']['schema']+'.'+json_line['dest']['table'],
                    'full':json_line['full'],
                    'if_exists':json_line['dest']['add_params']['if_exists'] if 'if_exists' in json_line['dest']['add_params'] else 'append',
                    'upsert':json_line['where']['add_params'] if 'add_params' in json_line['where'] else None,
                    'procedure':json_line['dest']['add_params']['procedure'] if 'procedure' in json_line['dest']['add_params'] else 'query'
                    },         
        # função de leitura
        python_callable=read_db,
        trigger_rule='none_failed',
        # manda o contexto da task para ser utilizada
        provide_context = True,
        dag=dag_name,
    )

    # definição do fluxo como um todo
    branch_task.set_downstream(where_task)
    where_task.set_downstream(select_task)
    branch_task.set_downstream(select_task)
    
    conj['SELECT_'+filename]={'flow':[]}
    conj['SELECT_'+filename]['flow'].append(branch_task)
    conj['SELECT_'+filename]['flow'].append(where_task)
    conj['SELECT_'+filename]['flow'].append(select_task)

    if len(json_line['dag']['dependency']) > 0:
        task_dependencies[task_names['branch']] = ['SELECT_'+item for item in json_line['dag']['dependency']]
        conj['SELECT_'+filename]['dependency'] = ['SELECT_'+item for item in json_line['dag']['dependency']]
    else:
        task_dependencies[task_names['branch']] = ['START']
        conj['SELECT_'+filename]['dependency'] = ['START']

    return task_dependencies
    #return {task_names['branch']:branch_task,task_names['where']:where_task,task_names['select']:select_task}

# Definicoes iniciais do DAG
args = {
            'owner': 'airflow',
            'start_date': airflow.utils.dates.days_ago(2),
            'retries': 0,
            'on_failure_callback': task_fail_slack_alert
        }
'''
# Criacao do DAG
dag = DAG(
    # Nome
    'dw_folder',
    # Intervalo de atualização
    #schedule_interval=timedelta(minutes=9)),
    schedule_interval="@daily",
    # Mata o DAG caso dê timeout
    dagrun_timeout=timedelta(minutes=60),
    # Procura nessa pasta por templates AirFlow
    template_searchpath=Variable.get('sql_path'),
    # Define os argumentos declarados anteriormente
    default_args=args,
    # define a função a ser chamada quando há erro
    on_failure_callback=task_fail_slack_alert,
    )


# Task para iniciar o fluxo, não realiza nenhuma ação
start = DummyOperator(
            task_id='START',
            dag=dag
        )

# Task para encerrar o fluxo, não realiza nenhuma ação
end = DummyOperator(
            task_id = 'END',
            trigger_rule='none_failed',
            dag=dag
        )
'''
#task_dependencies = {}

# Para cada arquivo SQL definido na pasta abaixo, serão criados
# tasks de acordo

path = Variable.get('sql_path')

params = mount_parameters(path)

dag_tasks = {}

conj = {}

schedule = '@daily'


print(params)
for item in enumerate(params):  
    print(item)  
    globals()[item[1]] = create_dag(item[1],schedule,args,item[1],params)