import duckdb
import datetime

now = datetime.datetime.now()

con = duckdb.connect(
    "/home/andy/Documents/petroleum_analytics/dbt_petroleum_analytics/prod.duckdb"
)
con.sql("select * from prod.reports.top_5_wells").write_csv(f"../top_5_wells_{now}.csv")
con.sql("select * from prod.reports.sum_by_basin").write_csv(
    f"../sum_by_basin_{now}.csv"
)
