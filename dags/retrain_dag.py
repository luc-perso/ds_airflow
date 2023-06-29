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
from airflow.operators.bash_operator import BashOperator
from airflow.models import Variable, DAG, XCom
from airflow.utils.db import provide_session
from docker.types import Mount

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from os.path import join
from github import Github, GithubException

import datetime
from dotenv import load_dotenv

from retraining.overwrite import overwrite_prod_model, build_path_model

load_dotenv()
HOST_DS_MLOPS_PATH = os.getenv('HOST_DS_MLOPS_PATH')
DS_MLOPS_PATH = os.getenv('DS_MLOPS_PATH')

STORAGE_PATH = os.getenv('STORAGE_PATH')
DB_STORAGE_PATH = os.getenv('DB_STORAGE_PATH')
PROD_MODEL_NAME = os.getenv('PROD_MODEL_NAME')

SENDER_MAIL = os.getenv('SENDER_MAIL')
SENDER_PASSWORD = os.getenv('SENDER_PASSWORD')
RECEPT_MAIL = os.getenv('RECEPT_MAIL')
TOKEN_GITHUB = os.getenv('TOKEN_GITHUB')

# Variable.set("retrained_model_name", 'bisous')

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
        print(retrained_model_name)
        Variable.set("retrained_model_name", retrained_model_name)
        # print(Variable.get("retrained_model_name"))


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
            print('IN')
            new_model_name = Variable.get("retrained_model_name")
            print(Variable.get("retrained_model_name"))
            print(new_model_name)
            overwrite_prod_model(STORAGE_PATH, DB_STORAGE_PATH, PROD_MODEL_NAME, new_model_name)
            return 'accurate'
        
        return 'inaccurate'
    

    def send_mail(subject, dest, mess):
        """
        Cette fonction permet d'envoyer des emails à partir d'un compte outlook
        """
        try:
            smtpObj = smtplib.SMTP('smtp-mail.outlook.com', 587)
        except Exception as e:
            print(e)
            smtpObj = smtplib.SMTP_SSL('smtp-mail.outlook.com', 465)

        msg = MIMEMultipart()
        msg['From'] = SENDER_MAIL
        msg['To'] = dest
        msg['Subject'] = subject
        message = mess
        msg.attach(MIMEText(message))

        smtpObj.ehlo()
        smtpObj.starttls()
        smtpObj.login(SENDER_MAIL, SENDER_PASSWORD)
        smtpObj.sendmail(SENDER_MAIL, dest, msg.as_string())

        smtpObj.quit()

    def send_mail_inaccurate(task_instance):
        new_f1_score = task_instance.xcom_pull(task_ids=['xCom_from_test_new_model'], key='new_model_f1_score')[0]
        prod_f1_score = task_instance.xcom_pull(task_ids=['xCom_from_test_prod_model'], key='prod_model_f1_score')[0]
        subject = "Retrain model failed !"
        mess = f'Training of the new model failed.\nThe new F1 score is {new_f1_score} compared to the previous score {prod_f1_score}.'

        send_mail(subject, RECEPT_MAIL, mess)

    def send_mail_accurate(task_instance):
        new_f1_score = task_instance.xcom_pull(task_ids=['xCom_from_test_new_model'], key='new_model_f1_score')[0]
        prod_f1_score = task_instance.xcom_pull(task_ids=['xCom_from_test_prod_model'], key='prod_model_f1_score')[0]
        subject = "Retrain model success !"
        mess = f'Training of the new model was successful.\nThe new F1 score is {new_f1_score} compared with the previous score of {prod_f1_score}.\nThe new model will now go into production.'

        send_mail(subject, RECEPT_MAIL, mess)


    def send_file_github(token, repo_name, filepath, filename):
        """
        Cette fonction permet d'envoyer un fichier directement sur le contenu sur un repo Github
        Si le fichier existe, il sera mise à jour sinon il sera créé
        """
        g = Github(token)
        repo = g.get_user().get_repo(repo_name)
        file = join(filepath, filename)

        with open(file, 'rb') as fichier:
            contenu = fichier.read()

        try:
            contents = repo.get_contents(filename, ref='main')
            repo.update_file(contents.path, "update file", contenu, contents.sha, branch="main")
        except GithubException:
            repo.create_file(filename, "commit new model", contenu, branch="main")
    
    def send_model_github(task_instance):
        prod_model_full_path, new_model_full_path, new_model_save_full_path = build_path_model(STORAGE_PATH, DB_STORAGE_PATH, PROD_MODEL_NAME, Variable.get("retrained_model_name"))
        send_file_github(TOKEN_GITHUB, 'DS_MLOPS', prod_model_full_path, new_model_save_full_path)


    task_load_data = BashOperator(
        task_id="load_data",
        env= {
            "AWS_ACCESS_KEY_ID": Variable.get("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": Variable.get("AWS_SECRET_ACCESS_KEY"),
            "AWS_DEFAULT_REGION": Variable.get("AWS_DEFAULT_REGION"),
        },
        bash_command='''\
            aws s3 cp s3://mlops-covid/data_test.zip /tmp/data_test.zip &&
            mkdir -p DS_MLOps/storage/input/db
            unzip /tmp/data_test.zip -d DS_MLOps/storage/input/db
        ''',
        dag=my_dag,
        doc_md="""
        # End

        Load dataset from aws s3
        """
    )

    # TODO:
    # use HOST_DS_MLOPS_PATH in a BachOperator to pull DS_MLOps repository
    # manage credential with secret value.
    task_clone_git = BashOperator(
        task_id="clone_git",
        bash_command= 'rm -rf DS_MLOps && git clone https://github.com/luc-perso/DS_MLOps.git',
        dag=my_dag,
        doc_md="""
        # End
        
        clone git DS_MLOPS github repository
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
    # manage credential with secret value.
    task_upload_model = PythonOperator(
        task_id='send_model_github',
        provide_context=True,
        python_callable=send_model_github,
        dag=my_dag
    )

    # send a message via email
    task_inaccurate_msg = PythonOperator(
        task_id='inaccurate',
        provide_context=True,
        python_callable=send_mail_inaccurate,
        dag=my_dag
    )


    # send a message via email following accurate
    task_accurate_msg = PythonOperator(
        task_id='accurate',
        provide_context=True,
        python_callable=send_mail_accurate,
        dag=my_dag
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



    task_clone_git >> task_load_data
    task_load_data >> task_retrain_model
    task_retrain_model >> task_variable_from_retrained_model
    task_variable_from_retrained_model >> [task_new_model, task_prod_model]
    task_new_model >> task_xCom_from_test_new_model
    task_prod_model >> task_xCom_from_test_prod_model
    [task_xCom_from_test_new_model, task_xCom_from_test_prod_model] >> task_select_model
    task_select_model >> [task_accurate_msg, task_inaccurate_msg]
    task_accurate_msg >> task_upload_model
    [task_upload_model, task_inaccurate_msg] >> task_end

