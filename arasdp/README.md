
# TSETMC Shareholder Daily Data Pipeline

This project implements an automated Airflow pipeline that collects end-of-day shareholder information for selected TSETMC instruments, stores the data as CSV files, and loads it into a PostgreSQL database.

A correct and validated API is used for data collection.

> Important Note
> The initially provided URL was incorrect:

```
https://tsetmc.com/History/{code}/{date}
```

The correct API endpoint is:

```
https://cdn.tsetmc.com/api/Shareholder/{code}/{date}
```

---

# Project Objectives

* Automatically fetch end-of-day shareholder data for each instrument
* Save the retrieved data into CSV files
* Load CSV data into a PostgreSQL table
* Run daily using Airflow scheduling
* Provide an extensible and modular ETL pipeline

---

# Pipeline Architecture

## Step 1 — Input

A JSON file named `ins_codes.json` contains the list of instrument codes to be processed:

```json
["34144395039913458", "12345678900000000"]
```

---

## Step 2 — Fetching Data

For each instrument code and each date in the date range, the following API call is made:

```
GET https://cdn.tsetmc.com/api/Shareholder/{code}/{date}
```

### Important: Filtering for “End-of-Day Shareholders”

The API may return historical rows containing `dEven` values **earlier** than the requested date.

Since the requirement is to collect **end-of-day** shareholder records, the pipeline skips any row where:

```
dEven < requested_date
```

The applied logic in code:

```python
if int(row['dEven'] < int(date_str)):
    continue
```

This ensures that only shareholder data for the exact date requested is stored.

---

## Step 3 — CSV Generation

Each date and each symbol produces a CSV file stored at:

```
dags/csvfiles/{code}_{date}.csv
```

CSV Columns:

```
symbolCode,date,shareHolderName,numberOfShares,perOfShares
```

---

## Step 4 — Load into PostgreSQL

Airflow reads each CSV and inserts the data into the database.

If the table does not exist, it will be created automatically.

---

# PostgreSQL Table Schema

```sql
CREATE TABLE IF NOT EXISTS tsetmc_history (
    symbolCode       VARCHAR(64),
    date             VARCHAR(64),
    shareHolderName  VARCHAR(128),
    numberOfShares   FLOAT,
    perOfShares      FLOAT
);
```

---

# API Response Structure

Example API response:

```json
{
  "shareShareholder": [
    {
      "shareHolderID": 0,
      "shareHolderName": "Company Example",
      "cIsin": "IRTKMOFD0006",
      "dEven": 20251129,
      "numberOfShares": 138347423.0,
      "perOfShares": 3.313,
      "change": 1,
      "changeAmount": 0.0,
      "shareHolderShareID": 24852038
    }
  ]
}
```

### Relevant fields used in the pipeline:

| Field              | Description               |
| ------------------ | ------------------------- |
| shareHolderName    | Name of the shareholder   |
| dEven              | Effective date (YYYYMMDD) |
| numberOfShares     | Number of shares          |
| perOfShares        | Percentage ownership      |
| shareHolderShareID | Unique identifier         |

---

# Project Folder Structure

```
.
├── dags/
│   ├── tsetmc_dag.py          # Airflow DAG file
│   ├── ins_codes.json         # List of instrument codes
│   └── csvfiles/              # Generated CSV files
│
├── config/
│   └── airflow.cfg            # Custom Airflow config (optional)
│
├── logs/                      # Airflow logs
├── plugins/                   # Optional plugins
├── docker-compose.yml         # Airflow + PostgreSQL environment
└── README.md
```

---

# Requirements

## Software

* Docker
* Docker Compose
* Airflow 3.x
* PostgreSQL 13+

## Airflow Connection

Create a PostgreSQL connection with:

**Connection ID:**

```
my_postgres
```

| Key      | Value       |
| -------- | ----------- |
| Host     | my_postgres |
| Schema   | data        |
| User     | data        |
| Password | data        |
| Port     | 5432        |

---

# Deployment

### 1. Clone and start the environment

```bash
docker compose up -d
```

### 2. Access Airflow

```
http://localhost:8080
username: admin
password: admin
```

### 3. Enable the DAG

Enable the DAG named:

```
tsetmc_data_pipeline
```

---

# API Notes

Incorrect initial URL:

```
https://tsetmc.com/History/{code}/{date}
```

Correct URL used in this project:

```
https://cdn.tsetmc.com/api/Shareholder/{code}/{date}
```

---

# Future Enhancements

* Add historical range expansion
* Store files in S3 or MinIO instead of local storage
* Add business rules for detecting shareholder ownership changes
* Add dashboards using Apache Superset or Metabase
* Support incremental updates and partitioned tables


