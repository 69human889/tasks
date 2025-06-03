# 📊 User Event Processing Pipeline

This project is a lightweight ETL (Extract, Transform, Load) pipeline built using **Python** and **Polars** to process user profile and event data. The pipeline cleans raw CSV/JSON input files, transforms them into a consistent format, joins user profiles with their activity events, and writes the final structured dataset to a partitioned **Parquet** file for efficient storage and querying.

---

## 🚀 Features

- Cleans malformed CSV user profile data
- Parses and transforms nested JSON event logs
- Extracts relevant fields and metadata (e.g., event date, button ID)
- Generates unique event IDs using UUIDs
- Joins user and event datasets on `user_id`
- Writes output as partitioned and compressed Parquet files

---

## 📁 Input Files

- `user_profiles.csv` – Raw user profile data with potential encoding issues.
- `user_events_20231026.json` – JSON-formatted event logs including nested fields under a `details` object.

---

## 🧱 Output

- `result_parquet/` – Partitioned Parquet files by `event_date`, compressed with **Snappy**.

---

## 🛠 Requirements

- Python 3.8+
- [Polars](https://pola.rs/) (`pip install polars`)
  
---

## 🧪 Usage

### 1. Prepare your environment

```bash
pip install polars
```
### 2. Place your input files

Ensure the following files are present in the same directory:

- `user_profiles.csv`
- `user_events_20231026.json`

### 3. Run the script
```bash
python __main__.py
```
This will:

- Clean the `user_profiles.csv` file and save it as `clean_user_profiles.csv`
- Parse and transform `user_events_20231026.json`
- Join user profile data with event data
- Save the final dataset to `result.parquet/`, partitioned by event date

---
## 📂 Code Structure

| Function | Description |
|---------|-------------|
| `clean_user_profile()` | Removes quotes and newline artifacts from the raw CSV |
| `load_profile()` | Loads the cleaned CSV into a Polars DataFrame with strict schema |
| `load_event_data()` | Loads and transforms JSON event data, flattening nested fields |
| `join_profile_and_events()` | Joins events with profile data using `user_id` |
| `write_to_parquet()` | Saves the joined DataFrame to a partitioned Parquet file |
| `main()` | Orchestrates the complete data pipeline |

---

## 📌 Notes

- The script assumes that all event records have a `details` field with specific subfields.
- Dates are parsed and used both for data enrichment and Parquet partitioning.
- Error handling and logging can be added for production use.

---

## 📃 License

MIT License. Feel free to use, modify, and distribute.
