import os
import sys
thisdir = os.path.dirname(__file__)
libdir = os.path.join(thisdir, '../DS_MLOps/lib/')

if libdir not in sys.path:
    sys.path.insert(0, libdir)

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable, DAG, XCom
from airflow.utils.db import provide_session
from docker.types import Mount

import datetime
from dotenv import load_dotenv

from retraining.overwrite import overwrite_prod_model

load_dotenv()
HOST_DS_MLOPS_PATH = os.getenv('HOST_DS_MLOPS_PATH')
DS_MLOPS_PATH = os.getenv('DS_MLOPS_PATH')

STORAGE_PATH = os.getenv('STORAGE_PATH')
DB_STORAGE_PATH = os.getenv('DB_STORAGE_PATH')
PROD_MODEL_NAME = os.getenv('PROD_MODEL_NAME')

Variable.set("retrained_model_name", '')

with DAG(
    dag_id='retrain_dag',
    doc_md="""
# Retrain DAG

This `DAG` is in charge to :

* collect new datas from our bucket
* retrain model
* if the new model is beter than the deployed one:
    * change the operationnal model
    * push modif on github

operation is triggered manually
""",
    tags=['mlops', 'datascientest'],
    schedule_interval=None,
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0),
    },
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    start_date=days_ago(0)
) as my_dag:

    @provide_session
    def clean_xcom(session=None, **context):
        dag = context["dag"]
        dag_id = dag._dag_id 
        session.query(XCom).filter(XCom.dag_id == dag_id).delete()


    # definition of the function to execute
    def read_xcom(task_instance, task_ids):
        print("Read XCOM from DockerOperator.. and maybe do things..")
        # time.sleep(1)
        # Grab xcom from previous task(dockerized task)..
        data = task_instance.xcom_pull(task_ids=task_ids, key='return_value')

        # print(data)
        # Airflow seems to be broken and will return all 'stdoout' as the XCOM traffic so we have to parse it or
        # write our code to only `print` the serialized thing we want.. in this case we are just printing a directionary.
        # if you do something like this and have string data in your return value.. either don't use
        # new lines for your stirngs or choose a different way to break things..

        xcom_data = data[0].split('\n')[-1]
        print("THIS IS WHAT I WANT:", xcom_data)

        return xcom_data
    

    def write_xCom(task_instance, var_name, value):
        task_instance.xcom_push(
            key=var_name,
            value=value
        )


    def variable_from_retrained_model(task_instance):
        retrained_model_name = read_xcom(task_instance, ['retrain_model'])
        # write_xCom(task_instance, 'retrained_model_name', retrained_model_name)
        Variable.set("retrained_model_name", retrained_model_name)


    def xCom_from_test_new_model(task_instance):
        new_model_f1_score = float(read_xcom(task_instance, ['test_new_model']))
        write_xCom(task_instance, 'new_model_f1_score', new_model_f1_score)


    def xCom_from_test_prod_model(task_instance):
        prod_model_f1_score = float(read_xcom(task_instance, ['test_prod_model']))
        write_xCom(task_instance, 'prod_model_f1_score', prod_model_f1_score)
    

    def select_model(task_instance):
        new_f1_score = task_instance.xcom_pull(task_ids=['xCom_from_test_new_model'], key='new_model_f1_score')[0]
        prod_f1_score = task_instance.xcom_pull(task_ids=['xCom_from_test_prod_model'], key='prod_model_f1_score')[0]

        print(new_f1_score)
        print(prod_f1_score)

        if new_f1_score > prod_f1_score:
            new_model_name = Variable.get("retrained_model_name")
            overwrite_prod_model(STORAGE_PATH, DB_STORAGE_PATH, PROD_MODEL_NAME, new_model_name)
            return 'accurate'
        
        return 'inaccurate'


    task_load_data = DummyOperator(
        task_id="load_data",
        dag=my_dag,
        doc_md="""
        # End

        Dummy task to mark load_data task todo
        """
    )

    # TODO:
    # use HOST_DS_MLOPS_PATH in a BachOperator to pull DS_MLOps repository
    # manage credential with secret value.
    task_clone_git = DummyOperator(
        task_id="clone_git",
        dag=my_dag,
        doc_md="""
        # End

        Dummy task to mark clone git task todo
        """
    )

    task_retrain_model = DockerOperator(
        task_id='retrain_model',
        image='luc074/ds_mlops:latest',
        api_version='1.37',
        auto_remove=True,
        environment= {
            "STORAGE_PATH": STORAGE_PATH,
            "DB_STORAGE_PATH": DB_STORAGE_PATH,
            "PROD_MODEL_NAME": PROD_MODEL_NAME,
        },
        mounts=[
            Mount(
                source=HOST_DS_MLOPS_PATH, 
                target=DS_MLOPS_PATH, 
                type='bind'
            )
        ],
        mount_tmp_dir=False,
        command=['python', os.path.join(DS_MLOPS_PATH, 'bin', 'retrain.py')],
        xcom_all=False,
        docker_url="TCP://docker-socket-proxy:2375",
        # docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        dag=my_dag,
        doc_md="""
        # End

        retrain of the model
        """
    )

    task_variable_from_retrained_model = PythonOperator(
        task_id='variable_from_retrained_model',
        provide_context=True,
        python_callable=variable_from_retrained_model,
        dag=my_dag
        )

    task_new_model = DockerOperator(
        task_id='test_new_model',
        image='luc074/ds_mlops:latest',
        api_version='1.37',
        auto_remove=True,
        environment= {
            "STORAGE_PATH": STORAGE_PATH,
            "DB_STORAGE_PATH": DB_STORAGE_PATH,
            "NEW_MODEL_NAME": Variable.get("retrained_model_name"),
        },
        mounts=[
            Mount(
                source=HOST_DS_MLOPS_PATH, 
                target=DS_MLOPS_PATH, 
                type='bind'
            )
        ],
        mount_tmp_dir=False,
        command=['python', os.path.join(DS_MLOPS_PATH, 'bin', 'score_new_model.py')],
        xcom_all=False,
        docker_url="TCP://docker-socket-proxy:2375",
        # docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        dag=my_dag,
        doc_md="""
        # End

        test the new model
        """
    )

    task_xCom_from_test_new_model = PythonOperator(
        task_id='xCom_from_test_new_model',
        provide_context=True,
        python_callable=xCom_from_test_new_model,
        dag=my_dag
        )

    task_prod_model = DockerOperator(
        task_id='test_prod_model',
        image='luc074/ds_mlops:latest',
        api_version='1.37',
        auto_remove=True,
        environment= {
            "STORAGE_PATH": STORAGE_PATH,
            "DB_STORAGE_PATH": DB_STORAGE_PATH,
            "PROD_MODEL_NAME": PROD_MODEL_NAME,
        },
        mounts=[
            Mount(
                source=HOST_DS_MLOPS_PATH, 
                target=DS_MLOPS_PATH, 
                type='bind'
            )
        ],
        mount_tmp_dir=False,
        command=['python', os.path.join(DS_MLOPS_PATH, 'bin', 'score_prod_model.py')],
        xcom_all=False,
        docker_url="TCP://docker-socket-proxy:2375",
        # docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        dag=my_dag,
        doc_md="""
        # End

        test the prod model
        """
    )

    task_xCom_from_test_prod_model = PythonOperator(
        task_id='xCom_from_test_prod_model',
        provide_context=True,
        python_callable=xCom_from_test_prod_model,
        dag=my_dag
    )

    task_select_model = BranchPythonOperator(
        task_id='select_model',
        provide_context=True,
        python_callable=select_model,
        dag=my_dag
    )
    
    # TODO:
    # use HOST_DS_MLOPS_PATH in a BachOperator to commit and push DS_MLOps repository
    # manage credential with secret value.
    task_accurate = DummyOperator(
        task_id='accurate'
    )

    # TODO:
    # send a message via Slack or email
    task_inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    # TODO:
    # send a message via Slack or email following accurate
    task_accurate_msg = DummyOperator(
        task_id='accurate_msg'
    )

    task_end = DummyOperator(
        task_id="end",
        trigger_rule='one_done',
        dag=my_dag,
        doc_md="""
        # End

        Dummy task to mark end of DAG
        """
    )



    task_load_data >> task_clone_git
    task_clone_git >> task_retrain_model
    task_retrain_model >> task_variable_from_retrained_model
    task_variable_from_retrained_model >> [task_new_model, task_prod_model]
    task_new_model >> task_xCom_from_test_new_model
    task_prod_model >> task_xCom_from_test_prod_model
    [task_xCom_from_test_new_model, task_xCom_from_test_prod_model] >> task_select_model
    task_select_model >> [task_accurate, task_inaccurate]
    task_accurate >> task_accurate_msg
    [task_accurate_msg, task_inaccurate] >> task_end

