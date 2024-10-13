import duckdb
import os

current_file_path = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file_path)
target_dir = os.path.dirname(current_dir)
print(target_dir)

conn = duckdb.connect(f"{target_dir}/prod.duckdb")

# Close the connection
conn.close()
