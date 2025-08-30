# 📊 Bonyan System – Data Engineering Task

## 📌 معرفی

این پروژه به‌عنوان یک **تسک مهندسی داده (Data Engineering)** طراحی شده و شامل پیاده‌سازی یک فرآیند **ETL (Extract, Transform, Load)** و تحلیل داده‌ها با استفاده از **SQL** است. داده‌های ورودی شامل **Call Detail Records (CDRs)** می‌باشند که اطلاعات مربوط به تماس‌ها، پیامک‌ها و مصرف دیتا مشترکین را در بر می‌گیرد.

---

## 🗂 ساختار داده (CSV ورودی)

فایل ورودی شامل رکوردهای مربوط به ترافیک مخابراتی است و ستون‌های آن به شرح زیر است:

| ستون           | توضیحات                               |
| -------------- | ------------------------------------- |
| timestamp      | زمان رخداد                            |
| caller\_msisdn | شماره تماس‌گیرنده                     |
| callee\_msisdn | شماره مقصد                            |
| event\_type    | نوع رخداد (sms, voice, data)          |
| caller\_city   | شهر تماس‌گیرنده                       |
| callee\_city   | شهر مقصد                              |
| duration       | مدت تماس (فقط برای voice)             |
| volume         | حجم دیتای مصرفی (فقط برای data)       |
| cost           | هزینه رخداد                           |
| is\_roaming    | آیا رویداد مربوط به رومینگ است یا خیر |

---

## 🎯 اهداف پروژه

### 1. فرآیند ETL

* **استخراج (Extract):** خواندن داده‌ها از فایل CSV
* **تبدیل (Transform):**

  * حذف رکوردهایی که در ستون `caller_msisdn` مقدار Null دارند.
  * حذف رکوردهایی که در رخداد **voice** ستون `duration` تهی است.
  * حذف رکوردهایی که در رخداد **data** ستون `volume` تهی است.
  * بررسی صحت داده‌ها:

    * `event_type` باید یکی از مقادیر {sms, voice, data} باشد.
    * شماره‌های تماس باید عددی باشند.
* **بارگذاری (Load):** ذخیره داده‌های پاک‌سازی‌شده در پایگاه داده PostgreSQL

### 2. تحلیل SQL

* 🔟 شهر برتر با بیشترین **مدت زمان تماس‌های voice**
* 🔟 شهر برتر با بیشترین **درآمد (cost)** به تفکیک نوع رویداد و مجموع کل (در بازه 2025-06-01 تا 2025-06-07)
* 🔟 مشترک برتر (`caller_msisdn`) بر اساس **کل هزینه رخدادها**
* 🔟 مشترک **Roamer** برتر بر اساس **مصرف دیتای roaming**

---

## 🛠 پیش‌نیازها

* **Python 3.8+**
* **PostgreSQL**
* کتابخانه‌های پایتون (در فایل `requirements.txt`):

  * `asyncpg`
  * `aiocsv`
  * `aiofiles`

---

## 🚀 نحوه اجرا

### 1. نصب وابستگی‌ها

```bash
pip install -r ./etl/requirements.txt
```

### 2. اجرای فرآیند ETL

```bash
python ./etl/
```

### 3. اتصال به دیتابیس PostgreSQL

اطلاعات اتصال:

* **Server:** `localhost`
* **Port:** `5432`
* **Database:** `subscriber_traffic`
* **User:** `tester`
* **Password:** `tester@321`

فایل داده ورودی:

```
./etl/subscriber_traffic.csv
```

---

## 📂 ساختار پروژه

```
bonyan_system/
│── etl/                     # ماژول‌های مربوط به ETL
│   ├── __main__.py
│   ├── db_operations.py
│   ├── env.py
│   ├── requirements.txt
│   ├── subscriber_traffic.csv
│── sql_queries/                     # کوئری‌های SQL تحلیل داده
│   ├── 1.sql      
│   ├── 2.sql
│   ├── 3.sql
│   ├── 4.sql
│   ├── create_table.sql
│── postgresql/
│   ├── docker-compose.yaml      
│── README.md                 # مستندات پروژه
```

---

## 🔮 بهبودهای آینده

* اضافه کردن Unit Test برای ETL
* استفاده از Airflow برای زمان‌بندی فرآیندها
* ساخت داشبورد تحلیلی با ابزارهایی مثل Tableau یا PowerBI

---

می‌خوای برات همین README رو به صورت **انگلیسی** هم بسازم (برای استفاده رسمی‌تر و بین‌المللی) یا همین نسخه فارسی کافی هست؟
