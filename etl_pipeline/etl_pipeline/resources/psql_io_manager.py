from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from dagster import IOManager, OutputContext, InputContext, io_manager
from sqlalchemy import create_engine, text
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession   # <— dùng để đảm bảo session luôn sẵn sàng

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
        self._spark = spark   # vẫn giữ nhưng KHÔNG còn bắt buộc

    def load_input(self, context: InputContext):
        schema = context.asset_key.path[0]
        table = context.asset_key.path[-1]

        with connect_psql(self._config) as db_conn:
            query = f'SELECT * FROM "{schema}"."{table}"'
            df = pd.read_sql(query, db_conn)

        # Nếu downstream dùng Spark thì convert luôn sang Spark DF
        if self._spark:
            return self._spark.createDataFrame(df)
        return df


    def handle_output(self, context: OutputContext, obj):
        schema = context.asset_key.path[0]
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
            # --- Pandas DataFrame ---
            for col in datetime_columns:
                if col in obj.columns:
                    obj[col] = pd.to_datetime(obj[col], errors="coerce")

            with connect_psql(self._config) as db_conn:
                with db_conn.connect() as cursor:
                    # Đảm bảo schema tồn tại
                    cursor.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

                    # Tạo bảng tạm giống schema gốc (nếu bảng gốc đã có)
                    cursor.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tmp_tbl}"'))
                    cursor.execute(
                        text(f'CREATE TABLE "{schema}"."{tmp_tbl}" (LIKE "{schema}"."{table}" INCLUDING ALL)')
                    )

                # Append data vào bảng tạm
                obj[list(ls_columns)].to_sql(
                    name=tmp_tbl,
                    con=db_conn,
                    schema=schema,
                    if_exists="append",
                    index=False,
                    chunksize=100000,
                    method="multi",
                )

                with db_conn.connect() as cursor:
                    self._do_upsert(context, cursor, schema, table, tmp_tbl, primary_keys, ls_columns)

        elif isinstance(obj, SparkDataFrame):
            # --- Spark DataFrame ---
            # CHANGED: không yêu cầu self._spark nữa; tự đảm bảo có session
            spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

            jdbc_url = (
                f"jdbc:postgresql://{self._config['host']}:{self._config['port']}/{self._config['database']}"
            )
            connection_props = {
                "user": self._config["user"],
                "password": self._config["password"],
                "driver": "org.postgresql.Driver",
            }

            # Đảm bảo schema tồn tại trước khi Spark ghi
            with connect_psql(self._config) as db_conn:
                with db_conn.connect() as cursor:
                    cursor.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))

            # Ghi thẳng Spark DF vào bảng tạm (overwrite tạo mới nếu chưa có)
            table_qualified = f'"{schema}"."{tmp_tbl}"'
            (
                obj.select(*ls_columns)
                .write
                .jdbc(
                    url=jdbc_url,
                    table=table_qualified,
                    mode="overwrite",
                    properties=connection_props,
                )
            )

            # Upsert từ bảng tạm vào bảng đích
            with connect_psql(self._config) as db_conn:
                with db_conn.connect() as cursor:
                    self._do_upsert(context, cursor, schema, table, tmp_tbl, primary_keys, ls_columns)

        else:
            raise TypeError("PostgreSQLIOManager only supports pandas.DataFrame or pyspark.sql.DataFrame")

    def _do_upsert(self, context, cursor, schema, table, tmp_tbl, primary_keys, ls_columns):
        # Kiểm tra số bản ghi bảng tạm
        result = cursor.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{tmp_tbl}"'))
        for row in result:
            context.log.info(f"Temp table records: {row[0]}")

        col_list = ",".join([f'"{c}"' for c in ls_columns])

        if len(primary_keys) > 0:
            conditions = " AND ".join(
                [f'"{schema}"."{table}"."{k}" = "{tmp_tbl}"."{k}"' for k in primary_keys]
            )
            command = f"""
            BEGIN;
            DELETE FROM "{schema}"."{table}"
            USING "{schema}"."{tmp_tbl}"
            WHERE {conditions};
            INSERT INTO "{schema}"."{table}" ({col_list})
            SELECT {col_list} FROM "{schema}"."{tmp_tbl}";
            COMMIT;
            """
        else:
            command = f"""
            BEGIN;
            TRUNCATE TABLE "{schema}"."{table}";
            INSERT INTO "{schema}"."{table}" ({col_list})
            SELECT {col_list} FROM "{schema}"."{tmp_tbl}";
            COMMIT;
            """

        cursor.execute(text(command))
        cursor.execute(text(f'DROP TABLE IF EXISTS "{schema}"."{tmp_tbl}"'))

# Resource IOManager (có cũng được, không có cũng ok sau khi sửa ở trên)
@io_manager(config_schema={
    "host": str,
    "port": int,
    "user": str,
    "password": str,
    "database": str,
})
def psql_io_manager(init_context):
    # Không bắt buộc, nhưng giữ để đảm bảo luôn có session sẵn
    spark = SparkSession.builder.getOrCreate()
    return PostgreSQLIOManager(init_context.resource_config, spark=spark)
