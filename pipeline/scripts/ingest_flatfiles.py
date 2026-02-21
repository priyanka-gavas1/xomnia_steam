# pipeline/scripts/ingest_flatfiles.py

import os
from pathlib import Path
from typing import List, Dict

from google.cloud import bigquery

# -------- CONFIG --------
PROJECT_ID = "xomnia-steam"
DATASET_ID = "raw_inbound"
TABLE_ID = "flatfile_data"

RAW_FILES_DIR = Path("pipeline/raw_files")

# encodings to try in order
ENCODINGS = ["utf-8", "windows-1252", "ascii"]


def read_file_lines(path: Path) -> List[str]:
    for enc in ENCODINGS:
        try:
            with path.open("r", encoding=enc) as f:
                return f.read().splitlines()
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError(f"Could not decode file {path} with encodings {ENCODINGS}")


def parse_line(line: str) -> (str, str):
    if "," in line:
        app_id, rest = line.split(",", 1)
        return app_id.strip(), rest.strip()
    else:
        return line.strip(), ""


def build_rows_for_file(file_path: Path) -> List[Dict]:
    flatfile_name = file_path.name
    lines = read_file_lines(file_path)

    rows = []
    for line in lines:
        if not line.strip():
            continue
        app_id, flatfile_content = parse_line(line)
        rows.append(
            {
                "flatfile_name": flatfile_name,
                "app_id": app_id,
                "flatfile_content": flatfile_content,
                # bq_insert_date is defaulted in table
            }
        )
    return rows


def load_rows_to_bigquery(rows: List[Dict]):
    if not rows:
        return

    import csv
    from tempfile import NamedTemporaryFile

    # Write rows to a temporary CSV file
    with NamedTemporaryFile(mode="w", delete=False, suffix=".csv") as tmp:
        writer = csv.writer(tmp)
        writer.writerow(["flatfile_name", "app_id", "flatfile_content"])  # header
        for r in rows:
            writer.writerow([r["flatfile_name"], r["app_id"], r["flatfile_content"]])
        temp_file_path = tmp.name

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition="WRITE_APPEND",
    )

    with open(temp_file_path, "rb") as f:
        load_job = client.load_table_from_file(f, table_ref, job_config=job_config)

    load_job.result()  # Wait for job to finish


def main():
    all_rows: List[Dict] = []

    for file_path in RAW_FILES_DIR.glob("*"):
        if file_path.is_file():
            rows = build_rows_for_file(file_path)
            all_rows.extend(rows)

    # you can batch per file instead if you prefer
    load_rows_to_bigquery(all_rows)


if __name__ == "__main__":
    main()