### ES234419 - Data Lakehouse

Semester Genap, 2024/2025
<br/>
Final Project

| **Name**                     | **NRP**    |
| ---------------------------- | ---------- |
| Nicholas Evan Sitanggang     | 5026231146 |
| Kayla Putri Maharani         | 5026231158 |
| Azrul Afif Syafaturahman     | 5026231166 |
| Tahiyyah Mufhimah            | 5026231170 |
| Muhammad Razan Parisya Putra | 5026231174 |

---

# ETL Transcript Pipeline

A Python-based ETL pipeline to extract academic transcript data from PDFs, transform it into structured format, and load it into a MySQL data warehouse using a star schema. Designed for scalable analytics on student performance and course outcomes.

---

## Requirements

* Python 3.8+
* MySQL server
* Libraries:

  * `pandas`
  * `mysql-connector-python`
  * `PyPDF2`

---

## Star Schema Structure

* `Dim_Mahasiswa`: Student dimension
* `Dim_MataKuliah`: Course dimension
* `Dim_Waktu`: Time dimension
* `Dim_Nilai`: Grade reference
* `Fact_Transkrip`: Transcript fact
* `Fact_Kelulusan`: Graduation performance
* `Fact_Prestasi_Semester`: Semester performance snapshot
* `Fact_Analisis_MataKuliah`: Aggregated course analytics

---

## How to Use

Follow these steps to run the ETL pipeline:

### 1. Prepare Your Environment

Ensure Python and MySQL are installed on your system.
Install required Python packages:

```bash
pip install -r requirements.txt
```


### 2. Configure Database Access

In the `etl.py` file, update the `DB_CONFIG` dictionary with your MySQL credentials:

```python
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'your_password',
    'database': 'transkrip'
}
```

> The database `transkrip` will be created automatically if it doesn't exist.

### 3. Add Source PDFs

Place your academic transcript PDF files in the `source/` folder (create it if it doesn’t exist):

```
your-project/
├── etl.py
├── README.md
├── source/
│   ├── student1.pdf
│   └── student2.pdf
```

### 4. Run the ETL Script

Simply run the script:

```bash
python etl.py
```

The script will:

* Connect to the MySQL database
* Create the required star schema tables (one-time)
* Extract, parse, and load transcript data
* Perform final aggregation for analytics


After execution:

* View logs in `transcript_etl.log`
* Check your MySQL database for updated tables and data
* Use the warehouse for queries, dashboards, or further analysis
