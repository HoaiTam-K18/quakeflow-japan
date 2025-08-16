from contextlib import contextmanager
from datetime import datetime

import pandas as pd
from dagster import IOManager, OutputContext, InputContext, io_manager
from sqlalchemy import create_engine, text

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
    def __init__(self, config):
        self._config = config

    def load_input(self, context: InputContext) -> pd.DataFrame:
        pass

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        schema = context.asset_key.path[-2]
        table = context.asset_key.path[-1]
        tmp_tbl = f"{table}_tmp_{datetime.now().strftime('%Y_%m_%d')}"

        # Metadata
        primary_keys = (context.metadata or {}).get("primary_keys", [])
        ls_columns = (context.metadata or {}).get("columns", obj.columns.tolist())
        datetime_columns = (context.metadata or {}).get("datetime_columns", [])

        # Format datetime columns
        for col in datetime_columns:
            if col in obj.columns:
                obj[col] = pd.to_datetime(obj[col], errors="coerce")

        with connect_psql(self._config) as db_conn:
            with db_conn.connect() as cursor:
                # Tạo bảng tạm giống bảng chính
                cursor.execute(
                    text(f'CREATE TEMP TABLE IF NOT EXISTS "{tmp_tbl}" (LIKE {schema}."{table}" INCLUDING ALL)')
                )

            # Đẩy dữ liệu vào bảng tạm
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
                # Kiểm tra số bản ghi bảng tạm
                result = cursor.execute(text(f'SELECT COUNT(*) FROM {schema}."{tmp_tbl}"'))
                for row in result:
                    context.log.info(f"Temp table records: {row[0]}")

                # upsert data
                if len(primary_keys) > 0:
                    conditions = " AND ".join(
                        [
                            f'{schema}.{table}."{k}" = {tmp_tbl}."{k}"'
                            for k in primary_keys
                        ]
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


                # Thực thi UPSERT
                cursor.execute(text(command))
                # Dọn bảng tạm
                cursor.execute(text(f'DROP TABLE IF EXISTS {schema}."{tmp_tbl}"'))