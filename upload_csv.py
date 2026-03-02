"""  
Upload CSV file to Snowflake internal stage
"""
import os

# Disable OCSP check BEFORE importing snowflake connector
os.environ['SNOWFLAKE_OCSP_FAIL_OPEN'] = 'true'

from dotenv import load_dotenv
import snowflake.connector
from utils.logger import get_logger

load_dotenv()
logger = get_logger("upload_csv")

CSV_FILE_PATH = os.path.join(os.path.dirname(__file__), "data", "sample_sales.csv")
STAGE_NAME = f"{os.getenv('SNOWFLAKE_DATABASE')}.{os.getenv('LANDING_SCHEMA')}.{os.getenv('FILE_STAGE')}"

def upload_csv():
    """Upload CSV file to Snowflake stage"""
    
    if not os.path.exists(CSV_FILE_PATH):
        logger.error(f"CSV file not found: {CSV_FILE_PATH}")
        return
    
    logger.info(f"Uploading {CSV_FILE_PATH} to stage {STAGE_NAME}")
    
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        insecure_mode=True
    )
    
    try:
        cursor = conn.cursor()
        file_path = CSV_FILE_PATH.replace("\\", "/")
        put_sql = f"PUT 'file://{file_path}' @{STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        cursor.execute(put_sql)
        logger.info("File uploaded successfully")
        
        cursor.execute(f"LIST @{STAGE_NAME}")
        files = cursor.fetchall()
        logger.info(f"Files in stage: {len(files)}")
        for file in files:
            logger.info(f"  - {file[0]}")
    finally:
        conn.close()

if __name__ == "__main__":
    upload_csv()
