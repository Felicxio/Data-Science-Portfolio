## 👤 Author

**Felicxio**  <!-- João Victor Assunção Pereira -->
- GitHub: [@Felicxio](https://github.com/Felicxio)
- LinkedIn: (www.linkedin.com/in/joão-victor-assunção-pereira-88a461211)
- Portfolio: [Data Science Portfolio](https://github.com/Felicxio/Data-Science-Portfolio)


# 📊 Sales ETL Pipeline - Northwind Database

Automated ETL (Extract, Transform, Load) pipeline for sales data analysis, processing 600K+ transactions from an 11-year period (2012-2023).

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![SQL](https://img.shields.io/badge/SQL-SQLite-lightgrey.svg)
![Pandas](https://img.shields.io/badge/Pandas-2.0+-green.svg)
![Status](https://img.shields.io/badge/Status-Complete-success.svg)

---

## 🎯 Project Overview

This project demonstrates **data engineering skills** by building a complete ETL pipeline that:
- Extracts data from SQLite database (SQL queries with multiple JOINs)
- Transforms and cleans 600K+ records
- Engineers 14 new features (temporal + business metrics)
- Loads processed data into CSV and multi-sheet Excel reports
- Runs end-to-end in ~25 seconds

**Key Skills Demonstrated:**
- SQL (complex queries, JOINs, aggregations)
- Python (OOP, pandas, data manipulation)
- ETL pipeline design
- Data quality validation
- Feature engineering

---

## 📁 Project Structure
```
sales-pipeline/
├── data/
│   ├── raw/              # SQLite database (downloaded by setup script)
│   └── processed/        # Output files (CSV + Excel reports)
├── src/
│   ├── setup_database.py # Downloads Northwind database
│   ├── extract.py        # SQL queries → pandas DataFrames
│   ├── transform.py      # Data cleaning + feature engineering
│   ├── load.py           # Saves processed data + reports
│   └── pipeline.py       # Orchestrates full ETL process
├── notebooks/            # Jupyter notebooks for analysis (optional)
├── requirements.txt      # Python dependencies
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Python 3.10+
- pip

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/[seu-usuario]/sales-pipeline.git
cd sales-pipeline
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Download database**
```bash
python src/setup_database.py
```

5. **Run ETL pipeline**
```bash
python src/pipeline.py
```

---

## 📊 Pipeline Workflow

### 1️⃣ **EXTRACT** (`extract.py`)
- Connects to SQLite database
- Executes SQL queries with 6-table JOINs
- Extracts 609,283 sales transactions
- **Key SQL concepts:** INNER JOIN, LEFT JOIN, GROUP BY, aggregations

### 2️⃣ **TRANSFORM** (`transform.py`)
- **Data Cleaning:**
  - Removes duplicates and invalid records
  - Validates data types and ranges
  - Handles missing values
  
- **Feature Engineering:**
  - **Temporal features** (8): Year, Month, Quarter, DayOfWeek, WeekOfYear, MonthName, YearMonth, DayName
  - **Business features** (6): OrderSize, HasDiscount, DiscountLevel, DeliveryDays, DeliverySpeed, RevenuePerUnit
  
- **Quality Report:** Generates data quality metrics

### 3️⃣ **LOAD** (`load.py`)
- Saves complete dataset (CSV: 178 MB, 607K records)
- Creates Excel report with 5 analysis sheets:
  - Summary (key metrics)
  - Category analysis
  - Monthly trends
  - Top 20 products
  - Top 20 customers
- Generates timestamped backup

---

## 📈 Key Results

| Metric | Value |
|--------|-------|
| **Total Records Processed** | 607,128 |
| **Time Period** | 11 years (2012-2023) |
| **Total Revenue** | $447 Million |
| **Orders Analyzed** | 16,282 |
| **Products** | 77 |
| **Customers** | 93 |
| **Pipeline Runtime** | ~25 seconds |

**Top Insights:**
- Beverages category leads with $92M revenue (20.6% of total)
- Average order value: $2,738
- Most deliveries are "Normal" speed (7-14 days)

---

## 🛠️ Technologies Used

- **Python 3.10+**
- **SQL (SQLite)**
- **Pandas** - Data manipulation
- **NumPy** - Numerical operations
- **OpenPyXL** - Excel file generation
- **Jupyter** - Exploratory analysis (optional)

---

## 📝 Sample Output

After running the pipeline, you'll find in `data/processed/`:
```
sales_complete.csv              # Full dataset (607K rows × 37 columns)
sales_reports.xlsx              # Excel with 5 analysis sheets
sales_complete_[timestamp].csv  # Versioned backup
pipeline_stats_[timestamp].json # Execution statistics
```

### Sample Excel Report Sheets:
1. **Summary** - Overall metrics (revenue, orders, customers)
2. **Categories** - Revenue breakdown by product category
3. **Monthly** - Monthly sales trends (136 months)
4. **Top Products** - Top 20 products by revenue
5. **Top Customers** - Top 20 customers by revenue

---

## 🎓 Learning Outcomes

This project demonstrates:

✅ **SQL Proficiency**
- Complex multi-table JOINs
- Aggregations (COUNT, SUM, AVG, MAX)
- Window functions and date operations

✅ **Python Data Engineering**
- Object-oriented programming (classes)
- ETL pipeline design patterns
- Error handling and logging

✅ **Data Quality**
- Data validation and cleaning
- Missing value handling
- Duplicate detection

✅ **Feature Engineering**
- Temporal feature extraction
- Business metric calculation
- Categorical binning

✅ **Production Best Practices**
- Modular code structure
- Virtual environments
- Version control ready

---

## 🚀 Future Enhancements

Possible extensions for this project:
- [ ] Add data visualization dashboard (Plotly/Streamlit)
- [ ] Implement customer segmentation (RFM analysis)
- [ ] Add predictive modeling (sales forecasting)
- [ ] Schedule automated pipeline runs (cron/Task Scheduler)
- [ ] Add unit tests (pytest)
- [ ] Deploy to cloud (AWS/GCP)

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

## 🙏 Acknowledgments

- Northwind database: Classic sample database for learning SQL
- Dataset source: [jpwhite3/northwind-SQLite3](https://github.com/jpwhite3/northwind-SQLite3)

---

**⭐ If you found this project useful, please consider giving it a star!**