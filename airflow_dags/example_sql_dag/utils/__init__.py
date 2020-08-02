"""
    Módulo de utilidades para a DAG
"""
from airflow_utils.postgres_dict import PostgresDictHook
import os
import pandas as pd
from pathlib import Path


def get_module(filename):
    """
        Esta função abstrai a resolução de nomes do diretório corrente e
        retorna algumas informações relevantes.

        :param filename: Diretório do arquivo a ter a resolução de nomes feita
        :type filename: str

        :return: Um dicionário com informações relevantes sobre o diretório do
            arquivo fornecido
        :rtype: dict
    """
    module_name = "-".join(Path(filename).parts[3:]).replace(".py", "")
    module_name = module_name.replace("-__init__", "")
    project_path = "/".join(Path(filename).parts[3:-1]).replace(".py", "")

    # Este diretório 'airflow' geralmente é o diretório raiz do projeto com
    # airflow, neste caso estamos apenas simulando o contexto das DAGs, mas
    # coloquei esta função aqui apenas para ajudar a entender o contexto da
    # documentação
    s3_path = os.path.join(
        "airflow", "/".join(Path(filename).parts[3:-1]).replace(".py", "")
    )
    return dict(
        module_name=module_name, project_path=project_path, s3_path=s3_path
    )


def read_local_file(file_relative_path):
    """
        Carrega o texto de um arquivo do disco em memória

        :param file_relative_path: Diretório relativo do arquivo
        :type file_relative_path: str

        :note: O arquivo deve possuir apenas texto

        :return: Texto contido no arquivo
        :rtype: str
    """
    home_path = os.environ["HOME"]
    file_path = os.path.join(home_path, "dags", file_relative_path)
    with open(file_path, "r") as file:
        file = file.read()
    return file


def read_sql(file_name):
    """
        Função para simplificar o processo de leitura em memória
        de arquivos SQL

        :param filename: Nome do arquivo SQL (Ex: filename.sql)
        :type filename: str

        :note: O arquivo deve estar dentro do diretório 'evaflow/d0/queries'

        :return: Conjunto de bytes que representa o arquivo lido
        :rtype: bytes
    """
    result = get_module(__file__)
    utils_path = result.get("project_path")
    # O project path lido daqui inclui este arquivo, voltando um nível,
    # podemos referenciar o diretório de queries. O split abaixo quebra
    # a string do diretório no último nível, apenas.
    project_path = os.path.split(utils_path)[0]

    print(f"Executando a leitura do arquivo {file_name}")
    sql_file_path = os.path.join(project_path, "queries", file_name)
    sql_file = read_local_file(sql_file_path)
    return sql_file


def run_sql(
    sql_file,
    bd_conn,
    database,
    pool,
    parameters=None,
    to_df=False,
):
    """
        Função para simplificar o processo de leitura de um arquivo SQL e
        a execução da respectiva consulta em um determinado Banco de dados

        :param sql_file: Nome do arquivo SQL (Ex: filename.sql)
        :type sql_file: str
        :param parameters: Dicionário eventuais parâmetros para a query
        :type parameters: dict
        :param bd_conn: Nome da conexão (Airflow) para o banco de dados
        :type bd_conn: str
        :param database: Nome do database cuja query deve ser executada
        :type database: str
        :param pool: Nome do Pool (Airflow) de conexões do banco de dados
        :type pool: str
        :param to_df: Flag que indica se o resultado da consulta deve
            ser convertido para Pandas Dataframe
        :type to_df: boolean

        :return: Caso (to_df=False) RealDictRow, no contrário pandas.Dataframe,
            com os resultados obtidos pela query
        :rtype: RealDictRow ou pandas.DataFrame
    """

    pg_hook = PostgresDictHook(
        postgres_conn_id=bd_conn,
        database=database,
        pool=pool,
    )

    sql_ = read_sql(sql_file)
    print(f"Executando a consulta contida no arquivo {sql_file}")
    pg_response = pg_hook.get_records(
        sql_,
        parameters=parameters,
        response_format="dict"
    )

    if to_df:
        print("Convertendo resposta do Hook em um Dataframe")
        pg_response = pd.DataFrame(pg_response)

    return pg_response
