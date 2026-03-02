# Retail Data Warehouse Pipeline

An end-to-end ETL pipeline that ingests retail sales data from CSV files into **Snowflake**, applying **Slowly Changing Dimension Type 2 (SCD2)** logic to all dimension tables to capture full historical changes.

## Architecture

```
CSV File
   │
   ▼
Landing Layer   (raw ingestion, no transformation)
   │
   ▼
Stage Layer     (views that clean & reshape landing data)
   │
   ▼
Target Layer    (SCD2 dimension tables + fact table)
```

## Project Structure

```
├── .env.example            # Template — copy to .env and fill credentials
├── requirements.txt
├── run_pipeline.py         # Orchestrates the full pipeline
├── sql/
│   └── ddl/
│       └── schema_setup.sql
├── utils/
│   ├── logger.py           # Logging setup (Python logging module)
│   └── db_connector.py     # Snowflake connection context manager
├── loaders/
│   ├── base_loader.py      # Abstract base class for all loaders
│   ├── extract_loader.py   # CSV → Landing
│   ├── dim_loaders/        # One file per dimension (SCD2)
│   └── fact_loaders/       # Fact table loaders
└── logs/                   # Auto-created at runtime
```

## Setup

### 1. Clone & install dependencies

```bash
git clone <repo-url>
cd retail_dwh_pipeline
python -m venv venv
# Windows
.\venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure credentials

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 3. Create Snowflake objects

Run `sql/ddl/schema_setup.sql` in your Snowflake worksheet.

### 4. Upload CSV to Snowflake stage

```sql
PUT file://path/to/sales.csv @RETAIL_DB.LANDING.CSV_STAGE;
```

### 5. Run the pipeline

```bash
python run_pipeline.py
```

## Star Schema

| Table               | Type      | SCD2 |
| ------------------- | --------- | ---- |
| `TGT_D_COUNTRY`     | Dimension | Yes  |
| `TGT_D_REGION`      | Dimension | Yes  |
| `TGT_D_STATE`       | Dimension | Yes  |
| `TGT_D_CITY`        | Dimension | Yes  |
| `TGT_D_CATEGORY`    | Dimension | Yes  |
| `TGT_D_SUBCATEGORY` | Dimension | Yes  |
| `TGT_D_SEGMENT`     | Dimension | Yes  |
| `TGT_D_SHIP_MODE`   | Dimension | Yes  |
| `TGT_D_PRODUCT`     | Dimension | Yes  |
| `TGT_D_CUSTOMER`    | Dimension | Yes  |
| `TGT_D_DATE`        | Dimension | No   |
| `TGT_F_SALES`       | Fact      | No   |
