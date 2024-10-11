import duckdb
import datetime

# con = duckdb.connect("dbt_petroleum_analytics/prod.duckdb")

# con.sql(
#     "create or replace table raw_data as (select * from read_csv('novi-data-engineer-assignment.csv') where api10 is not null)"
# )

now = datetime.datetime.now()

con = duckdb.connect()
con.sql(
    "create or replace table raw_data as (select * from read_csv('novi-data-engineer-assignment.csv') where api10 is not null)"
)

con.sql(f"select *, '{now}' as load_ts from raw_data").write_parquet(
    f"./dbt_petroleum_analytics/output_parquet/raw_data_no_nulls_{now}.parquet"
)
