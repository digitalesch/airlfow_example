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

# Hooks utilizados
from airflow.gcp.hooks.gsheets import GSheetsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.oracle_hook import OracleHook
from airflow.hooks.base_hook import BaseHook

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

# nome da conexão configurada no airflow, parametros estão abaixo
SLACK_CONN_ID = 'slack'

# função lambda para retornar os parametros incrementais / full para o template
return_string = lambda full,param_full,param_inc: param_full if full > 0 else param_inc
return_params = lambda type,param1,param2: param1 if type == 'db' else param2

# classe criada para obter as extensões que terão templates, o operador PythonOperator não apresenta tal atributo
class SQLTemplatedPythonOperator(PythonOperator):
    template_ext = ('.sql','.gs')

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
def return_records(type,hook,params):
    if type == 'db':
        return hook.get_records(params)
    else:
        return hook.get_values(range_=params['range'],value_render_option=params['value_render_option'])

# retorna o tipo de conexão necessária para identificar a origem do dado
# conn_info é um dicionario com o nome ("conn_id"), tipo "hook_type" e parametros adicionais
def return_hook(conn_info):
    if conn_info['hook_type'] == 'postgres':
        return {'type':'db','db_type':conn_info['hook_type'],'hook':PostgresHook(conn_info['conn_id'])}
    if conn_info['hook_type'] == 'oracle':
        return {'type':'db','db_type':conn_info['hook_type'],'hook':OracleHook(conn_info['conn_id'])}
    if conn_info['hook_type'] =='gsheets':
        return {'type':'gsheets','db_type':'','hook':GSheetsHook(gcp_conn_id = conn_info['conn_id'],spreadsheet_id = conn_info['add_params']['spreadsheet_id'],api_version = conn_info['add_params']['api_version'])}

    return None

def return_truncate_command(hook):
    if hook['type']=='db':
        if hook['db_type']=='postgres':
            return 'truncate '
        if hook['db_type']=='oracle':
            return 'delete '

    return None

def insert_values(conn_info,cursor,records):
    if conn_info['type']=='postgres':
        execute_values(cursor,'insert into '+conn_info['schema']+return_string(len(conn_info['schema']),'.','')+conn_info['table']+' VALUES %s',records)        
    if conn_info['type']=='oracle':        
        print('insert into '+conn_info['schema']+return_string(len(conn_info['schema']),'.','')+conn_info['table']+' '+conn_info['add_params']['col_definition']+' VALUES '+conn_info['add_params']['val_definition'],records)
        cursor.executemany('insert into '+conn_info['schema']+return_string(len(conn_info['schema']),'.','')+conn_info['table']+' '+conn_info['add_params']['col_definition']+' VALUES '+conn_info['add_params']['val_definition'],records)

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
    # teste, obtem a task anterior, a partir do contexto da task, análogo a entrar na seção "Task Instance Details" da task e obter os campos "Task Attributes"
    prev_task_id = context['task'].upstream_task_ids.pop()
    print('Prev task %s',prev_task_id)
    
    # source
    source = context.get('source')
    print(source)
    # dest
    dest = context.get('dest')
    print(dest)

    # retorna o hook de acordo com a conexão parametrizada
    data_hook = return_hook({'conn_id':source['conn'],'hook_type':source['type'],'add_params':source['add_params']})

    # printa no log a query já utilizando o template
    print(context['templates_dict']['query'])

    # Abre o arquivo passado como parametro e realiza a query indicada
    records = return_records(data_hook['type'],data_hook['hook'],return_params(data_hook['type'],context['templates_dict']['query'],source['add_params']))
    
    # coloca em tuplas, visto que o cursor precisa desse tipo dados para rodar o insert
    if data_hook['type'] == 'gsheets':
        results = []
        for item in records['values']:
            results.append(tuple(item))
        records = results
    
    # obtem o hook do destino
    data_hook_dest = return_hook({'conn_id':dest['conn'],'hook_type':dest['type'],'add_params':dest['add_params']})
        
    # cursor para utilizar escrita dos dados
    dest_conn = data_hook_dest['hook'].get_conn()
    dest_cursor = dest_conn.cursor()
    
    # caso seja full, deleta os dados (se houver)    
    if context['params']['full'] == 1:
        print(return_truncate_command(data_hook_dest))
        print(return_truncate_command(data_hook_dest)+context['params']['table']+';')
        
        data_hook_dest['hook'].run('delete from '+context['params']['table']+';')
        
    insert_values(dest,dest_conn.cursor(),records)
    
    # Da commit na base de destino
    dest_conn.commit()

    # limpeza de variaveis
    dest_conn.close()
    dest_cursor.close()    
    data_hook = None
    data_hook_dest = None

def create_dag(dag_id,schedule,dag_number,default_args,task_lists):
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)

    with dag:
        task_lists

    return dag

# Definicoes iniciais do DAG
args = {
            'owner': 'airflow',
            'start_date': airflow.utils.dates.days_ago(2),
            'retries': 0,
            'on_failure_callback': task_fail_slack_alert
        }

# Criacao do DAG
dag = DAG(
    # Nome
    'data_transfer_yaml',
    # Intervalo de atualização
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

task_dependencies = {}

# Para cada arquivo SQL definido na pasta abaixo, serão criados
# tasks de acordo

path = Variable.get('sql_path')

files = [(f.split('.')[0],path+f) for f in os.listdir(path) if re.match(r'.*\.(yaml|yml)', f)]

conj = {}

for file in files:
    stream = open(file[1])
    
    # le a primeira linha do sql, onde estão definidos os parametros de origem, destino, tipo de carga (full:0/1) e clausula where para o incremento
    config = yaml.safe_load(stream)

    
    #first_line = open(file).readline()

    # transforma a linha em um json
    json_line = config
    
    # Pega somente o nome do arquivo, ex: FT_INVOICE.yaml retorna FT_INVOICE
    filename = file[0]

    # nome das tasks utilizadas
    task_names = {
                    'tn1':'BRANCH_'+filename,
                    'tn2':'WHERE_'+filename,
                    'tn3':'SELECT_'+filename,
                }

    branch_task = BranchPythonOperator(
            task_id=task_names['tn1'],
            params = {
                    'full':json_line['full'],
                    'where_clause':json_line['where']['clause'],
                    'branch_where':task_names['tn2'],
                    'branch_select':task_names['tn3']
            },            
            provide_context=True,
            python_callable=return_branch,
            trigger_rule='none_failed',
            dag=dag,
        )

    # definição da task where, sendo que:
    # incremental: utiliza clausula where definida, caso contrário, é retornado comentário sql ('-----') para o template
    where_task = SQLTemplatedPythonOperator(
        # nome da task como definido acima
        task_id=task_names['tn2'],
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
        dag=dag
    )

    # task de seleção de dados
    select_task = SQLTemplatedPythonOperator(
        # campos que serão transformados, conforme o template definido no arquivo sql, carrega o arquivo de transformação
        templates_dict={
                             'query':json_line['query'] #filename+'.sql'                            
                        },
        # nome da task definido acima
        task_id=task_names['tn3'],
        # parametros de origem / destino da conexão
        op_kwargs = {
                        'source':json_line['source'],
                        'dest':json_line['dest'],
                    },
        # parametros de clausula where, definição de task anterior (pode ser obtida através de **context também dentro da função)
        # a clausula where, definida no campo 'where_clause' utiliza a lambda definida no começo para retorno de comentário (carga full) ou da clausula where (incremental)
        params =    {
                    'where_clause':return_string(json_line['full'],'------------',json_line['where']['clause']),
                    'prev_task':return_string(json_line['full'],task_names['tn1'],task_names['tn2']),
                    'table':json_line['dest']['schema']+'.'+json_line['dest']['table'],
                    'full':json_line['full']
                    }, 
        # função de leitura
        python_callable=read_db,
        trigger_rule='none_failed',
        # manda o contexto da task para ser utilizada
        provide_context = True,
        dag=dag
    )

    # definição do fluxo como um todo
    branch_task.set_downstream(where_task)
    where_task.set_downstream(select_task)
    branch_task.set_downstream(select_task)
    
    conj[filename]={'flow':[]}
    conj[filename]['flow'].append(branch_task)
    conj[filename]['flow'].append(where_task)
    conj[filename]['flow'].append(select_task)

    if len(json_line['dag']['dependency']) > 0:
        task_dependencies[task_names['tn1']] = ['SELECT_'+item for item in json_line['dag']['dependency']]
        conj[filename]['dependency'] = ['SELECT_'+item for item in json_line['dag']['dependency']]
    else:
        task_dependencies[task_names['tn1']] = ['START']
        conj[filename]['dependency'] = ['START']

# cria as interdependências de tasks, para que seja possível gerar stages e depois consumi-las
for dest_dependency in list(task_dependencies.keys()):    
    dest_instance = [dag.get_task(dest_dependency)]
    #instance[dest_dependency]=dest_instance
    for source_dependency in task_dependencies[dest_dependency]:
        source_instance = dag.get_task(source_dependency)        
        source_instance.set_downstream(dest_instance)
        #instance[dest_dependency].insert(0,source_dependency)

print(task_dependencies)
print(conj)

get_leafs_tasks(dag,['END']) >> end

#for i in range(1,5):
    #schedule = '@daily'

    #globals()[i] = create_dag('teste_'+str(i),schedule,i,args)