import os
import snowflake.connector

# Snowflake connection
conn = snowflake.connector.connect(
#    user="priyankagavas",
#    password="SNOWFLAKE_PASSWORD",
#    account="RR86738.europe-west4.gcp",
#    warehouse="PC_DBT_WH",
#    database="PC_DBT_WH",
#    schema="PC_DBT_WH"
#    conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WH"),
    database=os.getenv("SNOWFLAKE_DB"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)

cursor = conn.cursor()

# Folder containing your files
SOURCE_FOLDER = "pipeline/raw_files"

# Loop through all CSV files
for file_name in os.listdir(SOURCE_FOLDER):
    if not file_name.lower().endswith(".csv"):
        continue

    full_path = os.path.join(SOURCE_FOLDER, file_name)

    # Read file safely (ASCII + Windows-1252 + UTF-8)
    with open(full_path, "r", encoding="latin1", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")

            # Split into app_id and content
            if "," in line:
                app_id, flatfile_content = line.split(",", 1)
            else:
                app_id = line
                flatfile_content = ""

            # Insert into Snowflake
            cursor.execute("""
                INSERT INTO RAW.FLATFILE_DATA(flatfile_name, app_id, flatfile_content)
                VALUES (%s, %s, %s)
            """, (file_name, app_id, flatfile_content))

conn.commit()
cursor.close()
conn.close()