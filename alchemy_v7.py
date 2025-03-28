#with stramline loggers

import json
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, DateTime, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import requests

# Database connection URL
DATABASE_URL = "postgresql://postgres:Jio123@localhost:5433/postgres"

# Create engine & session
engine = create_engine(DATABASE_URL, future=True)
Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
session = Session()

# Reflect existing database tables
metadata = MetaData()
metadata.reflect(bind=engine)

AutoBase = automap_base(metadata=metadata)
AutoBase.prepare(autoload_with=engine)

# --- LOGGER CONFIGURATION ---
LOG_DIR = "loggers"
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("DatabaseLogger")
logger.setLevel(logging.DEBUG)  # Log all levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)

# File Handler (logs saved in a file)
file_handler = logging.FileHandler(os.path.join(LOG_DIR, "app.log"))
file_handler.setLevel(logging.INFO)  # Logs INFO and above to file

# Stream Handler (logs to console)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Logs all levels to console

# Formatter
log_format = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(log_format)
console_handler.setFormatter(log_format)

# Add handlers to logger
logger.addHandler(file_handler)
logger.addHandler(console_handler)

# Function to infer SQLAlchemy column types dynamically
def infer_sqlalchemy_type(value):
    if isinstance(value, int):
        return Integer
    elif isinstance(value, float):
        return Float
    elif isinstance(value, bool):
        return Boolean
    elif isinstance(value, str):
        return String(255)
    elif isinstance(value, datetime):
        return DateTime
    else:
        return String(255)

# Function to create a table if it does not exist
def create_table_if_not_exists(table_name, data):
    if table_name not in metadata.tables:
        logger.info(f"Creating table: {table_name}")

        columns = [Column("id", Integer, primary_key=True, autoincrement=True)]
        
        for key, value in data.items():
            if key != "id":
                col_type = infer_sqlalchemy_type(value)
                columns.append(Column(key, col_type))

        new_table = Table(table_name, metadata, *columns)
        new_table.create(engine)
        
        # Reflect the updated schema
        metadata.reflect(bind=engine)
        AutoBase.prepare(autoload_with=engine)

        return AutoBase.classes.get(table_name)
    
    return AutoBase.classes.get(table_name)

# Function to execute operations dynamically
def execute_operations(operations):
    try:
        for operation in operations:
            table_name = operation.get("table")
            op_type = operation.get("operation")
            data = operation.get("data", {})

            if not table_name or not op_type:
                logger.warning("Missing table name or operation type. Skipping...")
                continue

            TableClass = AutoBase.classes.get(table_name) or create_table_if_not_exists(table_name, data)

            if not TableClass:
                logger.error(f"Failed to create or retrieve table '{table_name}'. Skipping operation...")
                continue

            valid_columns = {col.name for col in TableClass.__table__.columns}
            incoming_columns = set(data.keys())

            # Check if any new column is present in data but not in table
            new_columns = incoming_columns - valid_columns
            if new_columns:
                logger.error(f"New columns detected in '{table_name}': {new_columns}. Rolling back operation.")
                session.rollback()
                continue  # Skip this operation

            filtered_data = {k: v for k, v in data.items() if k in valid_columns}

            if op_type == "create":
                try:
                    new_record = TableClass(**filtered_data)
                    session.add(new_record)
                    session.commit()
                    logger.info(f"Created record in '{table_name}': {filtered_data}")
                except TypeError as e:
                    logger.error(f"Error: {e}. Skipping invalid data for table '{table_name}'.")
                except IntegrityError:
                    session.rollback()
                    logger.warning(f"Record with the same ID already exists in '{table_name}'. Skipping.")

            elif op_type == "update":
                condition = operation.get("condition", {})

                query = session.query(TableClass)
                for key, value in condition.items():
                    query = query.filter(getattr(TableClass, key) == value)

                record = query.first()
                if record:
                    for key, value in filtered_data.items():
                        setattr(record, key, value)
                    session.commit()
                    logger.info(f"Updated record in '{table_name}': {filtered_data}")
                else:
                    logger.warning(f"Record with condition {condition} not found in '{table_name}'. Skipping update.")

            elif op_type == "delete":
                condition = operation.get("condition", {})

                query = session.query(TableClass)
                for key, value in condition.items():
                    query = query.filter(getattr(TableClass, key) == value)

                record = query.first()
                if record:
                    session.delete(record)
                    session.commit()
                    logger.info(f"Deleted record from '{table_name}' with condition {condition}")
                else:
                    logger.warning(f"Record with condition {condition} not found in '{table_name}'. Skipping delete.")

    except Exception as e:
        session.rollback()
        logger.error(f"Unexpected error: {e}")
    finally:
        session.close()
        logger.debug("Database session closed.")

# API call
url = "http://localhost:3000/file"
response = requests.get(url)
logger.info(f"Response Status Code: {response.status_code}")

try:
    json_data = response.json()
    logger.debug(f"Received JSON data: {json_data}")

    if "data" in json_data:
        execute_operations(json_data["data"])
    else:
        logger.error("Error: JSON response does not contain 'data' key.")
except requests.exceptions.JSONDecodeError:
    logger.error(f"Invalid JSON response received: {response.text}")
