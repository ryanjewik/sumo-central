from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
load_dotenv()


# S3_BUCKET = os.getenv('S3_BUCKET', 'your-default-bucket')
# S3_PREFIX = os.getenv('S3_PREFIX', 'your-default-prefix')
# AWS_REGION = os.getenv('AWS_REGION', 'us-west-2')
# AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY', 'your-access-key')
# AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY', 'your-secret-key')
# MONGO_URI = os.getenv('MONGO_URI', 'your-mongo-uri')
# MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'your-mongo-db-name')
# POSTGRES_DB = os.getenv('DB_NAME', 'your-postgres-db')
# POSTGRES_USER = os.getenv('DB_USERNAME', 'your-postgres-user')
# POSTGRES_PASSWORD = os.getenv('DB_PASSWORD', 'your-postgres-password')
# POSTGRES_HOST = os.getenv('DB_HOST', 'localhost')
# POSTGRES_PORT = os.getenv('DB_PORT', '5432')

def default_args(owner='airflow', retries=1, retry_delay_minutes=5):
    return {
        'owner': owner,
        'depends_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': retries,
        'retry_delay': timedelta(minutes=retry_delay_minutes),
    }