import datetime
import duckdb
import os

now = datetime.datetime.now()
current_dir = os.getcwd()

current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")

con = duckdb.connect("../prod.duckdb")

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
    f"""
copy top_5_wells to 's3://petroleum-data/output/top_5_wells_{now}.csv'
"""
)

con.sql(
    f"""
copy sum_by_basin to 's3://petroleum-data/output/sum_by_basin_{now}.csv'
"""
)

# con.sql("select * from prod.reports.top_5_wells").write_csv(f"../top_5_wells_{now}.csv")
# con.sql("select * from prod.reports.sum_by_basin").write_csv(
#     f"../sum_by_basin_{now}.csv"
# )
