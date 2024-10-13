import datetime
import duckdb
import os

now = datetime.datetime.now()

current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
target_dir = os.path.join(current_dir, "dbt_petroleum_analytics/output_parquet")

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

con = duckdb.connect()

con.sql("load httpfs")

con.sql(
    f"""
CREATE SECRET secret1 (
    TYPE S3,
    KEY_ID '{ACCESS_KEY}',
    SECRET '{SECRET_KEY}',
    REGION 'us-east-2'
)"""
)

con.sql(
    """CREATE TABLE raw_data as (
    SELECT *
    FROM 's3://petroleum-data/input/*.csv'
    where api10 is not null)
    """
)

con.sql(f"select *, '{now}' as load_ts from raw_data").write_parquet(
    f"{target_dir}/raw_data_no_nulls_{now}.parquet"
)
