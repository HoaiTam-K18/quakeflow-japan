from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from dagster import IOManager, OutputContext, InputContext, io_manager
from sqlalchemy import create_engine, text
from pyspark.sql import DataFrame as SparkDataFrame

@contextmanager
def connect_psql(config):
    conn_info = (
        f"postgresql+psycopg2://{config['user']}:{config['password']}"
        + f"@{config['host']}:{config['port']}"
        + f"/{config['database']}"
    )
    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class PostgreSQLIOManager(IOManager):
    def __init__(self, config, spark=None):
        self._config = config
        self._spark = spark   # SparkSession optional

    def load_input(self, context: InputContext):
        pass

    def handle_output(self, context: OutputContext, obj):
        schema = context.asset_key.path[-2]
        table = context.asset_key.path[-1]
        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"

        # Metadata
        primary_keys = (context.metadata or {}).get("primary_keys", [])
        ls_columns = (context.metadata or {}).get(
            "columns",
            obj.columns.tolist() if isinstance(obj, pd.DataFrame) else obj.columns
        )
        datetime_columns = (context.metadata or {}).get("datetime_columns", [])

        if isinstance(obj, pd.DataFrame):
            # --- Pandas DataFrame xử lý như cũ ---
            for col in datetime_columns:
                if col in obj.columns:
                    obj[col] = pd.to_datetime(obj[col], errors="coerce")

            with connect_psql(self._config) as db_conn:
                with db_conn.connect() as cursor:
                    cursor.execute(
                        text(f'CREATE TEMP TABLE IF NOT EXISTS "{tmp_tbl}" (LIKE {schema}."{table}" INCLUDING ALL)')
                    )

                obj[ls_columns].to_sql(
                    name=tmp_tbl,
                    con=db_conn,
                    schema=schema,
                    if_exists="replace",
                    index=False,
                    chunksize=100000,
                    method="multi",
                )

                with db_conn.connect() as cursor:
                    self._do_upsert(context, cursor, schema, table, tmp_tbl, primary_keys)

        elif isinstance(obj, SparkDataFrame):
            # --- Spark DataFrame ---
            if self._spark is None:
                raise ValueError("SparkSession is required to handle Spark DataFrame")

            jdbc_url = (
                f"jdbc:postgresql://{self._config['host']}:{self._config['port']}/{self._config['database']}"
            )
            connection_props = {
                "user": self._config["user"],
                "password": self._config["password"],
                "driver": "org.postgresql.Driver",
            }

            # Ghi thẳng vào bảng tạm (overwrite)
            (
                obj.select(*ls_columns)
                .write
                .jdbc(
                    url=jdbc_url,
                    table=f"{schema}.{tmp_tbl}",
                    mode="overwrite",
                    properties=connection_props,
                )
            )

            with connect_psql(self._config) as db_conn:
                with db_conn.connect() as cursor:
                    self._do_upsert(context, cursor, schema, table, tmp_tbl, primary_keys)

        else:
            raise TypeError("PostgreSQLIOManager only supports pandas.DataFrame or pyspark.sql.DataFrame")

    def _do_upsert(self, context, cursor, schema, table, tmp_tbl, primary_keys):
        # Kiểm tra số bản ghi bảng tạm
        result = cursor.execute(text(f'SELECT COUNT(*) FROM {schema}."{tmp_tbl}"'))
        for row in result:
            context.log.info(f"Temp table records: {row[0]}")

        if len(primary_keys) > 0:
            conditions = " AND ".join(
                [f'{schema}.{table}."{k}" = {tmp_tbl}."{k}"' for k in primary_keys]
            )
            command = f"""
            BEGIN;
            DELETE FROM {schema}.{table}
            USING {tmp_tbl}
            WHERE {conditions};
            INSERT INTO {schema}.{table}
            SELECT * FROM {tmp_tbl};
            COMMIT;
            """
        else:
            command = f"""
            BEGIN;
            TRUNCATE TABLE {schema}.{table};
            INSERT INTO {schema}.{table}
            SELECT * FROM {tmp_tbl};
            COMMIT;
            """

        cursor.execute(text(command))
        cursor.execute(text(f'DROP TABLE IF EXISTS {schema}."{tmp_tbl}"'))
