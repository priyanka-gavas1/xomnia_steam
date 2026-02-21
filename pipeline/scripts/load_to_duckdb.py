import duckdb
from pathlib import Path

DB_PATH = "pipeline/duckdb/steam.duckdb"
RAW_DIR = Path("pipeline/raw_files")

con = duckdb.connect(DB_PATH)

con.execute("""
CREATE TABLE IF NOT EXISTS bronze_flatfile_data (
    flatfile_name TEXT,
    app_id TEXT,
    flatfile_content TEXT,
    insert_ts TIMESTAMP DEFAULT now()
);
""")

for file in RAW_DIR.glob("*.csv"):
    flatfile_name = file.name
    print(f"Processing {flatfile_name}...")

    con.execute(f"""
        INSERT INTO bronze_flatfile_data (flatfile_name, app_id, flatfile_content)
        SELECT
            '{flatfile_name}',
            split_part(line, ',', 1),
            substr(line, length(split_part(line, ',', 1)) + 2)
        FROM read_csv_auto('{file}', columns={{'line': 'TEXT'}}, ignore_errors=true);
    """)

print("Step 2 completed.")