
# پایپلاین جمع‌آوری و ذخیره سهامداران TSETMC

این پروژه یک پایپلاین خودکار با Airflow است که داده‌های سهامداران نمادهای بورسی را از API صحیح TSETMC دریافت کرده و پس از ذخیره‌سازی در فایل‌های CSV، در PostgreSQL بارگذاری می‌کند.

> نکته مهم
> آدرس معرفی‌شده در ابتدا اشتباه بود:

```
https://tsetmc.com/History/{code}/{date}
```

اما API معتبر آدرس زیر است:

```
https://cdn.tsetmc.com/api/Shareholder/{code}/{date}
```

---

# اهداف پروژه

* دریافت خودکار سهامداران پایان روز برای هر نماد
* ذخیره‌سازی داده‌ها در CSV
* بارگذاری آن‌ها در PostgreSQL
* اجرای زمان‌بندی‌شده روزانه با Airflow
* قابل توسعه برای بازه‌های زمانی بزرگ‌تر

---

# معماری پایپلاین

## مرحله ۱: ورودی

فایل JSON شامل لیست ins_codes.

## مرحله ۲: دریافت داده

برای هر تاریخ و هر کد، داده‌ها از آدرس زیر دریافت می‌شوند:

```
https://cdn.tsetmc.com/api/Shareholder/{code}/{date}
```

### نکته مهم درباره “سهامداران پایان روز”

در پاسخ API ممکن است رکوردهایی وجود داشته باشد که `dEven` آن‌ها مربوط به **روزهای قبل** باشد، نه روز مورد درخواست.

چون هدف پروژه **دریافت سهامداران پایان همان روز** است، منطق زیر اضافه شده است:

```
اگر dEven < تاریخِ درخواست → آن رکورد نادیده گرفته شود
```

در کد:

```python
if int(row['dEven'] < int(date_str)):
    continue
```

این تضمین می‌کند فقط رکوردهای به‌روز همان تاریخ وارد CSV شوند.

---

# مرحله ۳: تولید CSV

برای هر تاریخ و هر نماد یک فایل CSV در مسیر:

```
dags/csvfiles/{code}_{date}.csv
```

---

# مرحله ۴: بارگذاری به PostgreSQL

فایل‌های CSV خوانده شده و در جدول `tsetmc_history` درج می‌شوند.

---

# شمای جدول (PostgreSQL)

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

# ساختار داده API

| فیلد               | توضیح                 |
| ------------------ | --------------------- |
| shareHolderName    | نام سهامدار           |
| cIsin              | کد ISIN               |
| dEven              | تاریخ                 |
| numberOfShares     | تعداد سهام            |
| perOfShares        | درصد مالکیت           |
| change             | تغییر نسبت به روز قبل |
| shareHolderShareID | شناسه یکتا            |

---

# ساختار پروژه

```
.
├── dags/
│   ├── tsetmc_dag.py
│   ├── ins_codes.json
│   └── csvfiles/
├── config/
├── logs/
├── plugins/
├── docker-compose.yml
└── README.md
```

---

# پیش‌نیازها

* Docker, Docker Compose
* Airflow 3.x
* اتصال PostgreSQL با شناسه `my_postgres`

---

# راه‌اندازی

### ۱. اجرای پروژه

```bash
docker compose up -d
```

### ۲. ورود به Airflow

```
http://localhost:8080
username: admin
password: <در لاگ airflow برای user جنریت میشود>
```

### ۳. فعال‌سازی DAG

`tsetmc_data_pipeline`

---

# نکات مهم API

URL اشتباه اولیه:

```
https://tsetmc.com/History/{code}/{date}
```

URL صحیح:

```
https://cdn.tsetmc.com/api/Shareholder/{code}/{date}
```

---

# توسعه در آینده

* پردازش داده‌ها برای یافتن تغییرات مالکیت
* بارگذاری در S3 یا دیتالیک
* ساخت داشبوردهای تحلیلی
* افزودن ستون‌های بیشتر از API

