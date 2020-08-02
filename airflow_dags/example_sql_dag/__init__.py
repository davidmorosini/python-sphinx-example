"""
    Módulo de inicialização da DAG
"""
from airflow import DAG
from airflow.operators.python_operator import (
    PythonOperator,
)
from datetime import datetime, timedelta
import pendulum

from airflow_dags.example_sql_dag.utils import (
    get_module,
    run_sql,
)

result = get_module(__file__)
module_name = result.get("module_name")
local_tz = pendulum.timezone("America/Sao_Paulo")

args = {
    "owner": "David",
    "depends_on_past": False,
    "start_date": datetime(2020, 7, 20, 0, 0, 0, tzinfo=local_tz),
    "provide_context": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    f"{module_name}", schedule_interval=None,
    default_args=args, catchup=False, max_active_runs=1
)


def exec_sql(**kwargs):
    """
        Python Operator para execução do SQL

        Este Operator cria uma conexão com o banco de dados selecionado
        através de um Hook PostgresSQL e realiza a consulta criada dentro
        do arquivo .sql indicado

        :note: Em operadores como este, os parâmetros são fornecidos
            automaticamente pelo Airflow
    """
    parameters = dict()
    response = run_sql(
        "example_query.sql",
        "airflow_db_conn",  # nome da connection para o database (Airflow)
        "schema_name",
        "airflow_pool_conn",  # Nome do pool para o banco (Airflow)
        parameters=parameters,
        to_df=True
    )
    print(f"O dataframe de responsa:\n {response}")


with dag:
    node_exec_Sql = PythonOperator(
        provide_context=True,
        task_id="exec_sql",
        python_callable=exec_sql
    )

node_exec_Sql
