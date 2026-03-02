# Retail Data Warehouse Pipeline

ETL pipeline that loads retail sales data into Snowflake with SCD2 dimension tracking.

## Setup

### 1. Prerequisites
- Python 3.10+
- Snowflake account

### 2. Install Dependencies

```bash
python -m venv venv
.\venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### 3. Configure Snowflake Credentials

Copy `.env.example` to `.env` and fill in your Snowflake credentials:

```bash
copy .env.example .env           # Windows
```

Edit `.env`:
```
SNOWFLAKE_ACCOUNT=your-account-identifier
SNOWFLAKE_USER=your-username
SNOWFLAKE_PASSWORD=your-password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=RETAIL_DB
```

### 4. Create Snowflake Schema

In Snowflake UI, select all text from `sql/ddl/schema_setup.sql` and click **Run All**.

This creates:
- Database: `RETAIL_DB`
- Schemas: `LANDING`, `STAGE`, `TEMP`, `TARGET`
- 10 dimension tables (with SCD2)
- 1 fact table
- All views and stage

## Run Pipeline

### Step 1: Upload CSV to Snowflake Stage

```bash
python upload_csv.py
```

Sample CSV is provided in `data/sample_sales.csv` (20 rows).

### Step 2: Run ETL Pipeline

```bash
python run_pipeline.py
```

This executes 12 steps:
1. Extract CSV → Landing table
2. Load Country dimension
3. Load Region dimension
4. Load State dimension
5. Load City dimension
6. Load Category dimension
7. Load Subcategory dimension
8. Load Segment dimension
9. Load Ship Mode dimension
10. Load Product dimension
11. Load Customer dimension
12. Load Sales fact table

## Expected Output

```
RETAIL DWH PIPELINE — START
Steps to run: 12
=================================================================
[OK] extract           3.2s
[OK] country           4.5s
[OK] region            7.1s
[OK] state             6.9s
[OK] city              5.1s
[OK] category          3.9s
[OK] subcategory       6.1s
[OK] segment           6.7s
[OK] ship_mode         8.0s
[OK] product           7.7s
[OK] customer         12.9s
[OK] sales            12.1s
-----------------------------------------------------------------
Total: 12 step(s) | Passed: 12 | Failed: 0 | Elapsed: 84.1s
=================================================================
```

## Project Structure

```
retail_dwh_pipeline/
├── data/                    # Sample CSV file
├── sql/ddl/                 # Snowflake schema setup
├── utils/                   # Database connector & logger
├── loaders/                 # ETL loaders
│   ├── dim_loaders/        # Dimension loaders (SCD2)
│   └── fact_loaders/       # Fact table loader
├── upload_csv.py           # Upload CSV to Snowflake
└── run_pipeline.py         # Main pipeline orchestrator
```

## Troubleshooting

**SSL Certificate Error:**
The `upload_csv.py` script includes `insecure_mode=True` to bypass SSL validation issues.

**Schema Not Found:**
Make sure to run the entire `schema_setup.sql` file using "Run All" in Snowflake UI.

**Pipeline Fails:**
Check logs in `logs/` directory for detailed error messages.
