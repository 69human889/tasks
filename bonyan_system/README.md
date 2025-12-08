# 📊 Bonyan System – Data Engineering Task

## 📌 Introduction

This project is designed as a **Data Engineering task** and includes the implementation of an **ETL (Extract, Transform, Load)** process along with data analysis using **SQL**.
The input dataset consists of **Call Detail Records (CDRs)**, which include information related to subscriber calls, SMS, and data usage.

---

## 🗂 Input Data Structure (CSV)

The input CSV file contains telecom traffic records with the following columns:

| Column        | Description                            |
| ------------- | -------------------------------------- |
| timestamp     | Event timestamp                        |
| caller_msisdn | Calling subscriber                     |
| callee_msisdn | Called subscriber                      |
| event_type    | Event type (sms, voice, data)          |
| caller_city   | Caller city                            |
| callee_city   | Callee city                            |
| duration      | Call duration (for voice events only)  |
| volume        | Data volume (for data events only)     |
| cost          | Event cost                             |
| is_roaming    | Indicates whether the event is roaming |

---

## 🎯 Project Objectives

### 1. ETL Process

**Extract**

* Read data from the CSV file

**Transform**

* Remove records where `caller_msisdn` is Null
* Remove **voice** records where `duration` is empty
* Remove **data** records where `volume` is empty
* Data validation:

  * `event_type` must be one of {sms, voice, data}
  * Phone numbers must be numeric

**Load**

* Insert the cleaned dataset into a PostgreSQL database

---

### 2. SQL Analysis

* Top 10 cities with the highest **voice call duration**
* Top 10 cities with the highest **revenue (cost)** by event type and total
  (in the range `2025-06-01` to `2025-06-07`)
* Top 10 subscribers (`caller_msisdn`) by **total event cost**
* Top 10 **Roaming subscribers** by **roaming data usage**

---

## 🛠 Requirements

* Python 3.8+
* PostgreSQL
* Python packages listed in `requirements.txt`:

  * `asyncpg`
  * `aiocsv`
  * `aiofiles`

---

## 🚀 How to Run

### 1. Install Dependencies

```bash
pip install -r ./etl/requirements.txt
```

### 2. Run the ETL Process

```bash
python ./etl/
```

### 3. PostgreSQL Connection Details

* **Server:** `localhost`
* **Port:** `5432`
* **Database:** `subscriber_traffic`
* **User:** `tester`
* **Password:** `tester@321`

Input data file:

```
./etl/subscriber_traffic.csv
```

---

## 📂 Project Structure

```
bonyan_system/
│── etl/                     
│   ├── __main__.py
│   ├── db_operations.py
│   ├── env.py
│   ├── requirements.txt
│   ├── subscriber_traffic.csv
│── sql_queries/             
│   ├── 1.sql      
│   ├── 2.sql
│   ├── 3.sql
│   ├── 4.sql
│   ├── create_table.sql
│── postgresql/
│   ├── docker-compose.yaml      
│── README.md
```

---

## 🔮 Future Improvements

* Add Unit Tests for ETL modules
* Use Airflow for process scheduling
* Build analytical dashboards using Tableau or PowerBI


