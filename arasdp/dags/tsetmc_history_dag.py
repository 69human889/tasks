from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Shared storage path - change this to your mounted path
SHARED_STORAGE_PATH = "/path/to/shared/storage"

# Load date range (last month until today)
start_date = (datetime.today().replace(day=1) - timedelta(days=1)).replace(day=1)  # first day last month
end_date = datetime.today()

@dag(
    schedule_interval="@daily",
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["example", "tsetmc"],
    description="Fetch tsetmc data and load to Postgres",
)
def tsetmc_data_pipeline():

    @task()
    def get_ins_codes():
        """
        Reads ins_codes from a JSON file.
        The file is expected to be in JSON list format, e.g. ["code1", "code2"]
        """
        file_path = "/path/to/ins_codes.json"  # Change this path to your file location
        with open(file_path, "r") as f:
            ins_codes = json.load(f)
        return ins_codes

    @task()
    def fetch_and_save_csv(ins_codes):
        """
        For each ins_code, fetch data for each date from start_date to today,
        save CSV files to shared storage.
        Returns list of file paths saved.
        """
        saved_files = []

        # Date range as strings YYYYMMDD
        date_range = pd.date_range(start=start_date, end=end_date)

        for code in ins_codes:
            for date in date_range:
                date_str = date.strftime("%Y%m%d")
                url = f"https://tsetmc.com/History/{code}/{date_str}"
                try:
                    response = requests.get(url)
                    response.raise_for_status()

                    # Assuming response is CSV text, if not you may need to parse differently
                    file_name = f"{code}_{date_str}.csv"
                    file_path = os.path.join(SHARED_STORAGE_PATH, file_name)

                    with open(file_path, "w", encoding='utf-8') as f:
                        f.write(response.text)

                    saved_files.append(file_path)
                except Exception as e:
                    print(f"Failed to fetch {url}: {e}")

        return saved_files

    @task()
    def load_csv_to_postgres(csv_files):
        """
        Load CSV files into Postgres database
        Assumes you have a Postgres connection set up in Airflow with conn_id="my_postgres"
        and table "tsetmc_data" exists with matching schema.
        """
        pg_hook = PostgresHook(postgres_conn_id="my_postgres")

        for file_path in csv_files:
            try:
                df = pd.read_csv(file_path)
                # Customize this based on your table structure:
                # Here we use to_sql with sqlalchemy engine from pg_hook
                engine = pg_hook.get_sqlalchemy_engine()
                df.to_sql("tsetmc_data", engine, if_exists="append", index=False)
                print(f"Loaded {file_path} to Postgres.")
            except Exception as e:
                print(f"Failed to load {file_path} into Postgres: {e}")

    ins_codes = get_ins_codes()
    csv_files = fetch_and_save_csv(ins_codes)
    load_csv_to_postgres(csv_files)

tsetmc_dag = tsetmc_data_pipeline()
