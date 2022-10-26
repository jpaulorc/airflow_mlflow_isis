"""
Atividade 02

Alunos:
    Bruno Alves dos Reis
    João Paulo Rodrigues Côrte
    Marcelo Moreira Ferreira da Silva

Professor:
    Jean Carlos Alves

Disciplina:
    CULTURA E PRÁTICAS DATAOPS E MLOPS
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

pathScript = "/opt/airflow/dags/scripts/"

DEFAULT_ARGS = {
    "owner": "Airflow",
    "depends_on_past": False,
    "start_date": datetime(2022, 1, 1),
    "retries": 0
}

with DAG("dag-pipeline-atividade-02",
         schedule_interval=timedelta(minutes=10),
         catchup=False,
         default_args=DEFAULT_ARGS) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("etl", tooltip="etl") as etl:

        t1 = BashOperator(dag=dag,
                          task_id="DownloadScriptPython",
                          bash_command="""
                          cd {0}
                          gdown 1NQZUV-rZOmdUyXyBM1ncDe0pR87x-4AO --output atividade_01.py
                          """.format(pathScript))
        
        t2 = BashOperator(dag=dag,
                          task_id="ExecutaScriptPython",
                          bash_command="""
                          cd {0}
                          python atividade_01.py
                          """.format(pathScript))

        [t1 >> t2]

    with TaskGroup("download", tooltip="download") as download:

        t1 = BashOperator(dag=dag,
                          task_id="DownloadLogisticModel",
                          bash_command="""
                          cd {0}
                          gdown 1UOx9aiYwINlUnYmdvl8uXWn6u1po0pa9 --output logistic_regression.py
                          """.format(pathScript))

        t2 = BashOperator(dag=dag,
                          task_id="DownloadKNNModel",
                          bash_command="""
                          cd {0}
                          gdown 1rfsFgWRUgGcv6y_7wfaDonTi2K3J5yPP --output knn.py
                          """.format(pathScript))

        t3 = BashOperator(dag=dag,
                          task_id="DownloadDecisionTreeModel",
                          bash_command="""
                          cd {0}
                          gdown 1VuBGmIdssebeePpWd4dmJpPeLbde3HPZ --output decision_tree.py
                          """.format(pathScript))

        t4 = BashOperator(dag=dag,
                          task_id="DownloadBestModel",
                          bash_command="""
                          cd {0}
                          gdown 1G65g-mHLa9zM_sllidVve8hpMsrRynwK --output best_model.py
                          """.format(pathScript))

        [t1 >> t2 >> t3 >> t4]

    with TaskGroup("model", tooltip="model") as model:

        t1 = BashOperator(dag=dag,
                          task_id="ExecuteLogisticModel",
                          bash_command="""
                          cd {0}
                          python logistic_regression.py
                          """.format(pathScript))

        t2 = BashOperator(dag=dag,
                          task_id="ExecuteKNNModel",
                          bash_command="""
                          cd {0}
                          python knn.py
                          """.format(pathScript))

        t3 = BashOperator(dag=dag,
                          task_id="ExecuteDecisionTreeModel",
                          bash_command="""
                          cd {0}
                          python decision_tree.py
                          """.format(pathScript))

        t4 = BashOperator(dag=dag,
                          task_id="ExecuteBestModel",
                          bash_command="""
                          cd {0}
                          python best_model.py
                          """.format(pathScript))

        [[t1, t2, t3] >> t4]                                                 

    end = DummyOperator(task_id="end")

    start >> etl >> download >> model >> end
