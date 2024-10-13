import datetime
import duckdb
import os

now = datetime.datetime.now()
current_dir = os.getcwd()

current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
parent_dir = os.path.dirname(current_dir)
target_dir = os.path.join(parent_dir, "prod.duckdb")

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
S3_BUCKET_NAME = "petroleum-data"

con = duckdb.connect(target_dir)
con.sql("INSTALL httpfs;")
con.sql("LOAD httpfs;")

con.sql(
    f"""
CREATE SECRET secret1 (
    TYPE S3,
    KEY_ID '{ACCESS_KEY}',
    SECRET '{SECRET_KEY}',
    REGION 'us-east-2'
)"""
)

# con.sql("select * from prod.stage.stg_dead_letter_queue").show()

con.sql(
    f"""
copy prod.reports.top_5_wells to 's3://{S3_BUCKET_NAME}/output/top_5_wells_{now}.csv';
"""
)

con.sql(
    f"""
copy prod.reports.sum_by_basin to 's3://{S3_BUCKET_NAME}/output/sum_by_basin_{now}.csv';
"""
)
