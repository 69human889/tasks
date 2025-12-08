from airflow.decorators import dag, task
from airflow.models import Variable
import requests
import json
import os
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
import csv
import sqlalchemy
from lxml import html
# Shared storage path - change this to your mounted path
SHARED_STORAGE_PATH = "dags/csvfiles/"


# Load date range (last month until today)

@dag(
    schedule="@daily",
    start_date=datetime.today() - timedelta(days=30),
    catchup=True,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=1),
    },
    tags=["task", "tsetmc"],
    description="Fetch tsetmc data and load to Postgres",
)
def tsetmc_data_pipeline():

    @task()
    def get_ins_codes():
        """
        Reads ins_codes from a JSON file.
        The file is expected to be in JSON list format, e.g. ["code1", "code2"]
        """
        file_path = "dags/ins_codes.json"  # Change this path to your file location
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
        date_range = [datetime.today() - timedelta(days=i) for i in range(10, -1, -1)]


        for code in ins_codes:
            for date in date_range:
                date_str = date.strftime("%Y%m%d")
                url = f"https://cdn.tsetmc.com/api/Shareholder/{code}/{date_str}"

                response = requests.get(url,verify=False)
                data = ['symbolCode,date,shareHolderName,numberOfShares,perOfShares\n']

                for row in response.json().get('shareShareholder'):
                    if int(row['dEven']<int(date_str)): 
                        continue
                    data.append(','.join([str(code),str(date),str(row['shareHolderName']),str(row['numberOfShares']),str(row['perOfShares'])])+'\n')

                file_name = f"{code}_{date_str}.csv"
                file_path = os.path.join(SHARED_STORAGE_PATH, file_name)

                with open(file_path, "w", encoding='utf-8') as f:
                    f.writelines(data)

                saved_files.append(file_path)


        return saved_files

    @task()
    def load_csv_to_postgres(csv_files):
        """
        Load CSV files into Postgres database
        Assumes you have a Postgres connection set up in Airflow with conn_id="my_postgres"
        and table "tsetmc_data" exists with matching schema.
        """
        pg_hook = PostgresHook(postgres_conn_id="my_postgres")
        engine = pg_hook.get_sqlalchemy_engine()
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("CREATE TABLE if not exists tsetmc_history (symbolCode varchar(64),date varchar(64),shareHolderName varchar(128),numberOfShares float,perOfShares float)"))
            for file_path in csv_files:
                with open(file_path,'r') as file:
                    reader = csv.DictReader(file)    
    
                    # Customize this based on your table structure:
                    # Here we use to_sql with sqlalchemy engine from pg_hook
                    
                        
                    conn.execute(
                    sqlalchemy.text("INSERT INTO tsetmc_history (symbolCode,date,shareHolderName,numberOfShares,perOfShares) VALUES (:symbolCode, :date, :shareHolderName, :numberOfShares, :perOfShares)"),
                        list(map(dict,reader)),
                    )
                    print(f"Loaded {file_path} to Postgres.")


    ins_codes = get_ins_codes()
    csv_files = fetch_and_save_csv(ins_codes)
    load_csv_to_postgres(csv_files)

tsetmc_dag = tsetmc_data_pipeline()
