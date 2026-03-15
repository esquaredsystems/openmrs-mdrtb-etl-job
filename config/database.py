from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv()


def get_source_engine():
    return create_engine(
        f"mysql+pymysql://{os.getenv('SOURCE_DB_USER')}:{os.getenv('SOURCE_DB_PASS')}"
        f"@{os.getenv('SOURCE_DB_HOST')}:{os.getenv('SOURCE_DB_PORT')}/{os.getenv('SOURCE_DB_NAME')}"
    )


def get_target_engine():
    return create_engine(
        f"mysql+pymysql://{os.getenv('TARGET_DB_USER')}:{os.getenv('TARGET_DB_PASS')}"
        f"@{os.getenv('TARGET_DB_HOST')}:{os.getenv('TARGET_DB_PORT')}/{os.getenv('TARGET_DB_NAME')}"
    )