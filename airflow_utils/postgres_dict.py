# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from builtins import str
from datetime import datetime
from contextlib import closing
from typing import Optional
import os
import psycopg2
import psycopg2.extras
import psycopg2.extensions
from sqlalchemy import create_engine
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException


class DbApiDictHook(BaseHook):
    """
    Abstract base class for sql hooks.
    """

    response_formats = {"matrix": None, "dict": psycopg2.extras.RealDictCursor}
    # Override to provide the connection name.
    conn_name_attr = None  # type: Optional[str]
    # Override to have a default connection id for a particular dbHook
    default_conn_name = "default_conn_id"
    # Override if this db supports autocommit.
    supports_autocommit = False
    # Override with the object that exposes the connect method
    connector = None

    def __init__(self, *args, **kwargs):
        if not self.conn_name_attr:
            raise AirflowException("conn_name_attr is not defined")
        elif len(args) == 1:
            setattr(self, self.conn_name_attr, args[0])
        elif self.conn_name_attr not in kwargs:
            setattr(self, self.conn_name_attr, self.default_conn_name)
        else:
            setattr(self, self.conn_name_attr, kwargs[self.conn_name_attr])

    def get_conn(self):
        """Returns a connection object
        """
        db = self.get_connection(getattr(self, self.conn_name_attr))
        return self.connector.connect(
            host=db.host, port=db.port, username=db.login, schema=db.schema
        )

    def get_uri(self):
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        login = ""
        if conn.login:
            login = "{conn.login}:{conn.password}@".format(conn=conn)
        host = conn.host
        if conn.port is not None:
            host += ":{port}".format(port=conn.port)
        return "{conn.conn_type}://{login}{host}/{conn.schema}".format(
            conn=conn, login=login, host=host
        )

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        if engine_kwargs is None:
            engine_kwargs = {}
        return create_engine(self.get_uri(), **engine_kwargs)

    def get_pandas_df(self, sql, parameters=None):
        """
        Executes the sql and returns a pandas dataframe

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        import pandas.io.sql as psql

        with closing(self.get_conn()) as conn:
            return psql.read_sql(sql, con=conn, params=parameters)

    def get_records(self, sql, parameters=None, response_format="matrix"):
        """
        Executes the sql and returns a set of records.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """

        with closing(self.get_conn()) as conn:
            with closing(
                conn.cursor(
                    cursor_factory=self.response_formats[response_format]
                )
            ) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchall()

    def get_first(self, sql, parameters=None, response_format="matrix"):
        """
        Executes the sql and returns the first resulting row.

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        with closing(self.get_conn()) as conn:
            with closing(
                conn.cursor(
                    cursor_factory=self.response_formats[response_format]
                )
            ) as cur:
                if parameters is not None:
                    cur.execute(sql, parameters)
                else:
                    cur.execute(sql)
                return cur.fetchone()

    def run(
        self, sql, autocommit=False, parameters=None, response_format="matrix"
    ):
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially

        :param sql: the sql statement to be executed (str) or a list of
            sql statements to execute
        :type sql: str or list
        :param autocommit: What to set the connection"s autocommit setting to
            before executing the query.
        :type autocommit: bool
        :param parameters: The parameters to render the SQL query with.
        :type parameters: mapping or iterable
        """
        if isinstance(sql, str):
            sql = [sql]

        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, autocommit)

            with closing(
                conn.cursor(
                    cursor_factory=self.response_formats[response_format]
                )
            ) as cur:
                for s in sql:
                    if parameters is not None:
                        self.log.info(
                            "{} with parameters {}".format(s, parameters)
                        )
                        cur.execute(s, parameters)
                    else:
                        self.log.info(s)
                        cur.execute(s)

            # If autocommit was set to False for db that supports autocommit,
            # or if db does not supports autocommit, we do a manual commit.
            if not self.get_autocommit(conn):
                conn.commit()

    def set_autocommit(self, conn, autocommit):
        """
        Sets the autocommit flag on the connection
        """
        if not self.supports_autocommit and autocommit:
            self.log.warn(
                (
                    "%s connection doesn't support "
                    "autocommit but autocommit activated."
                ),
                getattr(self, self.conn_name_attr),
            )
        conn.autocommit = autocommit

    def get_autocommit(self, conn):
        """
        Get autocommit setting for the provided connection.
        Return True if conn.autocommit is set to True.
        Return False if conn.autocommit is not set or set to False or conn
        does not support autocommit.

        :param conn: Connection to get autocommit setting from.
        :type conn: connection object.
        :return: connection autocommit setting.
        :rtype: bool
        """

        return getattr(conn, "autocommit", False) and self.supports_autocommit

    def get_cursor(self, response_format="matrix"):
        """
        Returns a cursor
        """
        return self.get_conn().cursor(
            cursor_factory=self.response_formats[response_format]
        )

    def insert_rows(
        self,
        table,
        rows,
        target_fields=None,
        commit_every=1000,
        replace=False,
        response_format="matrix",
    ):
        """
        A generic way to insert a set of tuples into a table,
        a new transaction is created every commit_every rows

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        :param replace: Whether to replace instead of insert
        :type replace: bool
        """
        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = "({})".format(target_fields)
        else:
            target_fields = ""
        i = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(
                conn.cursor(
                    cursor_factory=self.response_formats[response_format]
                )
            ) as cur:
                for i, row in enumerate(rows, 1):
                    lst = []
                    for cell in row:
                        lst.append(self._serialize_cell(cell, conn))
                    values = tuple(lst)
                    placeholders = ["%s"] * len(values)
                    if not replace:
                        sql = "INSERT INTO "
                    else:
                        sql = "REPLACE INTO "
                    sql += "{0} {1} VALUES ({2})".format(
                        table, target_fields, ",".join(placeholders)
                    )
                    cur.execute(sql, values)
                    if commit_every and i % commit_every == 0:
                        conn.commit()
                        self.log.info("Loaded %s into %s rows so far", i, table)

            conn.commit()
        self.log.info("Done loading. Loaded a total of %s rows", i)

    @staticmethod
    def _serialize_cell(cell, conn=None):
        """
        Returns the SQL literal of the cell as a string.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The serialized cell
        :rtype: str
        """

        if cell is None:
            return None
        if isinstance(cell, datetime):
            return cell.isoformat()
        return str(cell)

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file

        :param table: The name of the source table
        :type table: str
        :param tmp_file: The path of the target file
        :type tmp_file: str
        """
        raise NotImplementedError()

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table

        :param table: The name of the target table
        :type table: str
        :param tmp_file: The path of the file to load into the table
        :type tmp_file: str
        """
        raise NotImplementedError()


class PostgresDictHook(DbApiDictHook):
    """
    Interact with Postgres.
    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    For Redshift, also use redshift in the extra connection parameters and
    set it to true. The cluster-identifier is extracted from the beginning of
    the host field, so is optional. It can however be overridden in the extra field.
    extras example: ``{"iam":true, "redshift":true, "cluster-identifier": "my_cluster_id"}``
    """

    conn_name_attr = "postgres_conn_id"
    default_conn_name = "postgres_default"
    supports_autocommit = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(self):
        conn = self.get_connection(self.postgres_conn_id)

        # check for authentication via AWS IAM
        if conn.extra_dejson.get("iam", False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)

        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.schema or conn.schema,
            port=conn.port,
        )
        # check for ssl parameters in conn.extra
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name in [
                "sslmode",
                "sslcert",
                "sslkey",
                "sslrootcert",
                "sslcrl",
                "application_name",
                "keepalives_idle",
            ]:
                conn_args[arg_name] = arg_val

        self.conn = psycopg2.connect(**conn_args)
        return self.conn

    def copy_expert(self, sql, filename, open=open):
        """
        Executes SQL using psycopg2 copy_expert method.
        Necessary to execute COPY command without access to a superuser.

        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        if not os.path.isfile(filename):
            with open(filename, "w"):
                pass

        with open(filename, "r+") as f:
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql, f)
                    f.truncate(f.tell())
                    conn.commit()

    def bulk_load(self, table, tmp_file):
        """
        Loads a tab-delimited file into a database table
        """
        self.copy_expert(
            "COPY {table} FROM STDIN".format(table=table), tmp_file
        )

    def bulk_dump(self, table, tmp_file):
        """
        Dumps a database table into a tab-delimited file
        """
        self.copy_expert("COPY {table} TO STDOUT".format(table=table), tmp_file)

    @staticmethod
    def _serialize_cell(cell, conn):
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.

        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :type cell: object
        :param conn: The database connection
        :type conn: connection object
        :return: The cell
        :rtype: object
        """
        return cell

    def get_iam_token(self, conn):
        """
        Uses AWSHook to retrieve a temporary password to connect to Postgres
        or Redshift. Port is required. If none is provided, default is used for
        each service
        """
        from airflow.contrib.hooks.aws_hook import AwsHook

        redshift = conn.extra_dejson.get("redshift", False)
        aws_conn_id = conn.extra_dejson.get("aws_conn_id", "aws_default")
        aws_hook = AwsHook(aws_conn_id)
        login = conn.login
        if conn.port is None:
            port = 5439 if redshift else 5432
        else:
            port = conn.port
        if redshift:
            # Pull the custer-identifier from the beginning of the Redshift URL
            # ex. my-cluster.ccdre4hpd39h.us-east-1.redshift.amazonaws.com returns my-cluster
            cluster_identifier = conn.extra_dejson.get(
                "cluster-identifier", conn.host.split(".")[0]
            )
            client = aws_hook.get_client_type("redshift")
            cluster_creds = client.get_cluster_credentials(
                DbUser=conn.login,
                DbName=self.schema or conn.schema,
                ClusterIdentifier=cluster_identifier,
                AutoCreate=False,
            )
            token = cluster_creds["DbPassword"]
            login = cluster_creds["DbUser"]
        else:
            client = aws_hook.get_client_type("rds")
            token = client.generate_db_auth_token(conn.host, port, conn.login)
        return login, token, port


class PostgresDictOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in ".sql"
    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        sql,
        postgres_conn_id="postgres_default",
        autocommit=False,
        parameters=None,
        database=None,
        response_format="matrix",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.response_format = response_format

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        self.hook = PostgresDictHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        self.hook.run(
            self.sql,
            self.autocommit,
            parameters=self.parameters,
            response_format=self.response_format,
        )
        for output in self.hook.conn.notices:
            self.log.info(output)


class PostgresDictOperatorXcom(BaseOperator):
    """
    Executes sql code in a specific Postgres database. SQL should return
        a single value to be used with Xcom
    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: string
    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in ".sql"
    :param database: name of database which overwrite defined one in connection
    :type database: string
    :param default_value: value to be used when the SQL query returns Null
    :type default_value: any
    """

    template_fields = ("sql",)
    template_ext = (".sql",)
    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        sql,
        postgres_conn_id="postgres_default",
        autocommit=False,
        parameters=None,
        database=None,
        default_value=None,
        response_format="matrix",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.default_value = default_value
        self.response_format = response_format

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        hook = PostgresDictHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )

        result = hook.get_records(
            self.sql,
            parameters=self.parameters,
            response_format=self.response_format,
        )
        self.log.info(f"Value returned: {result}")
        if result is None:
            result = self.default_value
        return result


class PostgresDictPlugin(AirflowPlugin):
    name = "postgres_dict"
    operators = [PostgresDictOperator, PostgresDictOperatorXcom]
    # A list of class(es) derived from BaseHook
    hooks = [PostgresDictHook, DbApiDictHook]
    # A list of class(es) derived from BaseExecutor
    executors = []
    # A list of references to inject into the macros namespace
    macros = []
    # A list of objects created from a class derived
    # from flask_admin.BaseView
    admin_views = []
    # A list of Blueprint object created from flask.Blueprint
    flask_blueprints = []
    # A list of menu links (flask_admin.base.MenuLink)
    menu_links = []
