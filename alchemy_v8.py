# import json
# import os
# import logging
# from datetime import datetime
# from sqlalchemy import create_engine, inspect, Column, Integer, String, Boolean, Float, DateTime, MetaData, Table
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.automap import automap_base
# from sqlalchemy.exc import IntegrityError, SQLAlchemyError
# import requests

# # Database connection URL
# DATABASE_URL = "postgresql://postgres:Jio123@localhost:5433/postgres"

# # Create engine & session
# engine = create_engine(DATABASE_URL, future=True)
# Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
# session = Session()

# # Reflect existing database tables
# metadata = MetaData()
# metadata.reflect(bind=engine)

# AutoBase = automap_base(metadata=metadata)
# AutoBase.prepare(autoload_with=engine)

# # Log directory
# LOG_DIR = "logs"
# os.makedirs(LOG_DIR, exist_ok=True)

# # Logger setup function
# def setup_logger(name, filename):
#     logger = logging.getLogger(name)
#     logger.setLevel(logging.INFO)

#     file_handler = logging.FileHandler(os.path.join(LOG_DIR, filename))
#     stream_handler = logging.StreamHandler()

#     formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     file_handler.setFormatter(formatter)
#     stream_handler.setFormatter(formatter)

#     logger.addHandler(file_handler)
#     logger.addHandler(stream_handler)

#     return logger

# # Create separate loggers
# loggers = {
#     "created": setup_logger("created", "created_records.log"),
#     "updated": setup_logger("updated", "updated_records.log"),
#     "deleted": setup_logger("deleted", "deleted_records.log"),
#     "failures": setup_logger("failures", "failures.log"),
#     "skipped": setup_logger("skipped", "skipped_operations.log")
# }

# # Function to infer SQLAlchemy column types dynamically
# def infer_sqlalchemy_type(value):
#     if isinstance(value, int):
#         return Integer
#     elif isinstance(value, float):
#         return Float
#     elif isinstance(value, bool):
#         return Boolean
#     elif isinstance(value, str):
#         return String(255)
#     elif isinstance(value, datetime):
#         return DateTime
#     else:
#         return String(255)

# # Function to create a table if it does not exist
# def create_table_if_not_exists(table_name, data):
#     if table_name not in metadata.tables:
#         loggers["failures"].info(f"Creating table: {table_name}")

#         columns = [Column("id", Integer, primary_key=True, autoincrement=True)]
#         for key, value in data.items():
#             if key != "id":
#                 col_type = infer_sqlalchemy_type(value)
#                 columns.append(Column(key, col_type))

#         new_table = Table(table_name, metadata, *columns)
#         new_table.create(engine)

#         metadata.reflect(bind=engine)
#         AutoBase.prepare(autoload_with=engine)

#         return AutoBase.classes.get(table_name)

#     return AutoBase.classes.get(table_name)

# # Function to execute operations dynamically
# def execute_operations(operations):
#     try:
#         for operation in operations:
#             table_name = operation.get("table")
#             op_type = operation.get("operation")
#             data = operation.get("data", {})

#             if not table_name or not op_type:
#                 loggers["skipped"].error("Missing table name or operation type. Skipping operation.")
#                 continue

#             TableClass = AutoBase.classes.get(table_name) or create_table_if_not_exists(table_name, data)

#             if not TableClass:
#                 loggers["failures"].error(f"Failed to create or retrieve table '{table_name}'. Skipping.")
#                 continue

#             valid_columns = {col.name for col in TableClass.__table__.columns}
#             incoming_columns = set(data.keys())

#             # Check if new unexpected columns exist
#             new_columns = incoming_columns - valid_columns
#             if new_columns:
#                 loggers["failures"].error(f"New columns detected in '{table_name}': {new_columns}. Rolling back.")
#                 session.rollback()
#                 continue  # Skip operation

#             filtered_data = {k: v for k, v in data.items() if k in valid_columns}

#             if op_type == "create":
#                 try:
#                     new_record = TableClass(**filtered_data)
#                     session.add(new_record)
#                     session.commit()
#                     loggers["created"].info(f"Created record in '{table_name}': {filtered_data}")
#                 except IntegrityError:
#                     session.rollback()
#                     loggers["skipped"].warning(f"Duplicate record in '{table_name}'. Skipping.")
#                 except TypeError as e:
#                     loggers["failures"].error(f"Type error in '{table_name}': {e}")

#             elif op_type == "update":
#                 condition = operation.get("condition", {})

#                 query = session.query(TableClass)
#                 for key, value in condition.items():
#                     query = query.filter(getattr(TableClass, key) == value)

#                 record = query.first()
#                 if record:
#                     for key, value in filtered_data.items():
#                         setattr(record, key, value)
#                     session.commit()
#                     loggers["updated"].info(f"Updated record in '{table_name}': {filtered_data}")
#                 else:
#                     loggers["skipped"].warning(f"Record not found in '{table_name}' for update.")

#             elif op_type == "delete":
#                 condition = operation.get("condition", {})

#                 query = session.query(TableClass)
#                 for key, value in condition.items():
#                     query = query.filter(getattr(TableClass, key) == value)

#                 record = query.first()
#                 if record:
#                     session.delete(record)
#                     session.commit()
#                     loggers["deleted"].info(f"Deleted record from '{table_name}' with condition {condition}")
#                 else:
#                     loggers["skipped"].warning(f"Record not found in '{table_name}' for delete.")
#     except Exception as e:
#         session.rollback()
#         loggers["failures"].error(f"Unexpected error: {e}")
#     finally:
#         session.close()
#         loggers["failures"].info("Database session closed.")

# # API call
# url = "http://localhost:3000/file"
# response = requests.get(url)
# loggers["failures"].info(f"Response Status Code: {response.status_code}")

# try:
#     json_data = response.json()
#     loggers["failures"].info(f"JSON data received: {json_data}")

#     if "data" in json_data:
#         execute_operations(json_data["data"])
#     else:
#         loggers["failures"].error("JSON response does not contain 'data' key.")
# except requests.exceptions.JSONDecodeError:
#     loggers["failures"].error(f"Invalid JSON response received: {response.text}")




#==============



# import json
# import os
# import logging
# from datetime import datetime
# from sqlalchemy import create_engine, inspect, Column, Integer, String, Boolean, Float, DateTime, MetaData, Table
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy.ext.automap import automap_base
# from sqlalchemy.exc import IntegrityError, SQLAlchemyError
# import requests

# # Database connection URL
# DATABASE_URL = "postgresql://postgres:Jio123@localhost:5433/postgres"

# # Create engine & session
# engine = create_engine(DATABASE_URL, future=True)
# Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
# session = Session()

# # Reflect existing database tables
# metadata = MetaData()
# metadata.reflect(bind=engine)

# AutoBase = automap_base(metadata=metadata)
# AutoBase.prepare(autoload_with=engine)

# # Log directory
# LOG_DIR = "logs"
# os.makedirs(LOG_DIR, exist_ok=True)

# # Logger setup function
# def setup_logger(name, filename, level=logging.INFO):
#     logger = logging.getLogger(name)
#     logger.setLevel(level)

#     file_handler = logging.FileHandler(os.path.join(LOG_DIR, filename))
#     formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
#     file_handler.setFormatter(formatter)

#     logger.addHandler(file_handler)

#     return logger

# # Create separate loggers
# loggers = {
#     "created": setup_logger("created", "created_records.log"),
#     "updated": setup_logger("updated", "updated_records.log"),
#     "deleted": setup_logger("deleted", "deleted_records.log"),
#     "failures": setup_logger("failures", "failures.log"),
#     "skipped": setup_logger("skipped", "skipped_operations.log"),
#     "info": setup_logger("info", "info.log", logging.INFO)  # Info log in a separate file
# }

# # Function to infer SQLAlchemy column types dynamically
# def infer_sqlalchemy_type(value):
#     if isinstance(value, int):
#         return Integer
#     elif isinstance(value, float):
#         return Float
#     elif isinstance(value, bool):
#         return Boolean
#     elif isinstance(value, str):
#         return String(255)
#     elif isinstance(value, datetime):
#         return DateTime
#     else:
#         return String(255)

# # Function to create a table if it does not exist
# def create_table_if_not_exists(table_name, data):
#     if table_name not in metadata.tables:
#         loggers["info"].info(f"Creating table: {table_name}")

#         columns = [Column("id", Integer, primary_key=True, autoincrement=True)]
#         for key, value in data.items():
#             if key != "id":
#                 col_type = infer_sqlalchemy_type(value)
#                 columns.append(Column(key, col_type))

#         new_table = Table(table_name, metadata, *columns)
#         new_table.create(engine)

#         metadata.reflect(bind=engine)
#         AutoBase.prepare(autoload_with=engine)

#         return AutoBase.classes.get(table_name)

#     return AutoBase.classes.get(table_name)

# # Function to execute operations dynamically
# def execute_operations(operations):
#     try:
#         for operation in operations:
#             table_name = operation.get("table")
#             op_type = operation.get("operation")
#             data = operation.get("data", {})

#             if not table_name or not op_type:
#                 loggers["skipped"].error("Missing table name or operation type. Skipping operation.")
#                 continue

#             TableClass = AutoBase.classes.get(table_name) or create_table_if_not_exists(table_name, data)

#             if not TableClass:
#                 loggers["failures"].error(f"Failed to create or retrieve table '{table_name}'. Skipping.")
#                 continue

#             valid_columns = {col.name for col in TableClass.__table__.columns}
#             incoming_columns = set(data.keys())

#             # Check if new unexpected columns exist
#             new_columns = incoming_columns - valid_columns
#             if new_columns:
#                 loggers["failures"].error(f"New columns detected in '{table_name}': {new_columns}. Rolling back.")
#                 session.rollback()
#                 continue  # Skip operation

#             filtered_data = {k: v for k, v in data.items() if k in valid_columns}

#             if op_type == "create":
#                 try:
#                     new_record = TableClass(**filtered_data)
#                     session.add(new_record)
#                     session.commit()
#                     loggers["created"].info(f"Created record in '{table_name}': {filtered_data}")
#                 except IntegrityError:
#                     session.rollback()
#                     loggers["skipped"].warning(f"Duplicate record in '{table_name}'. Skipping.")
#                 except TypeError as e:
#                     loggers["failures"].error(f"Type error in '{table_name}': {e}")

#             elif op_type == "update":
#                 condition = operation.get("condition", {})

#                 query = session.query(TableClass)
#                 for key, value in condition.items():
#                     query = query.filter(getattr(TableClass, key) == value)

#                 record = query.first()
#                 if record:
#                     for key, value in filtered_data.items():
#                         setattr(record, key, value)
#                     session.commit()
#                     loggers["updated"].info(f"Updated record in '{table_name}': {filtered_data}")
#                 else:
#                     loggers["skipped"].warning(f"Record not found in '{table_name}' for update.")

#             elif op_type == "delete":
#                 condition = operation.get("condition", {})

#                 query = session.query(TableClass)
#                 for key, value in condition.items():
#                     query = query.filter(getattr(TableClass, key) == value)

#                 record = query.first()
#                 if record:
#                     session.delete(record)
#                     session.commit()
#                     loggers["deleted"].info(f"Deleted record from '{table_name}' with condition {condition}")
#                 else:
#                     loggers["skipped"].warning(f"Record not found in '{table_name}' for delete.")
#     except Exception as e:
#         session.rollback()
#         loggers["failures"].error(f"Unexpected error: {e}")
#     finally:
#         session.close()
#         loggers["info"].info("Database session closed.")

# # API call
# url = "http://localhost:3000/file"
# response = requests.get(url)
# loggers["info"].info(f"Response Status Code: {response.status_code}")

# try:
#     json_data = response.json()
#     loggers["info"].info(f"JSON data received: {json_data}")

#     if "data" in json_data:
#         execute_operations(json_data["data"])
#     else:
#         loggers["failures"].error("JSON response does not contain 'data' key.")
# except requests.exceptions.JSONDecodeError:
#     loggers["failures"].error(f"Invalid JSON response received: {response.text}")

#=======================================================

import json
import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, inspect, Column, Integer, String, Boolean, Float, DateTime, MetaData, Table
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

# Log directory
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Logger setup function
def setup_logger(name, filename, level=logging.INFO):
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # File Handler
    file_handler = logging.FileHandler(os.path.join(LOG_DIR, filename))
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)

    # Console Handler (logs to terminal)
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter('%(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

# Create separate loggers
loggers = {
    "created": setup_logger("created", "created_records.log"),
    "updated": setup_logger("updated", "updated_records.log"),
    "deleted": setup_logger("deleted", "deleted_records.log"),
    "failures": setup_logger("failures", "failures.log"),
    "skipped": setup_logger("skipped", "skipped_operations.log"),
    "info": setup_logger("info", "info.log", logging.INFO)  # Info log in a separate file
}

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
        loggers["info"].info(f"Creating table: {table_name}")

        columns = [Column("id", Integer, primary_key=True, autoincrement=True)]
        for key, value in data.items():
            if key != "id":
                col_type = infer_sqlalchemy_type(value)
                columns.append(Column(key, col_type))

        new_table = Table(table_name, metadata, *columns)
        new_table.create(engine)

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
                loggers["failures"].error(f"Missing table name '{table_name}' or operation type '{op_type}'. Skipping operation.")
                continue

            TableClass = AutoBase.classes.get(table_name) or create_table_if_not_exists(table_name, data)

            if not TableClass:
                loggers["failures"].error(f"Failed to create or retrieve table '{table_name}'. Skipping.")
                continue

            valid_columns = {col.name for col in TableClass.__table__.columns}
            incoming_columns = set(data.keys())

            # Check if new unexpected columns exist
            new_columns = incoming_columns - valid_columns
            if new_columns:
                loggers["failures"].error(f"New columns detected in '{table_name}': {new_columns}. Rolling back.")
                session.rollback()
                continue  # Skip operation

            filtered_data = {k: v for k, v in data.items() if k in valid_columns}

            if op_type == "create":
                try:
                    new_record = TableClass(**filtered_data)
                    session.add(new_record)
                    session.commit()
                    loggers["created"].info(f"Created record in '{table_name}': {filtered_data}")
                except IntegrityError:
                    session.rollback()
                    loggers["skipped"].warning(f"Duplicate record in '{table_name}'('{filtered_data}'). Skipping.")
                except TypeError as e:
                    loggers["failures"].error(f"Type error in '{table_name}': {e}")

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
                    loggers["updated"].info(f"Updated record in '{table_name}': {filtered_data} with condition {condition}")
                else:
                    loggers["skipped"].warning(f"Record not found in '{table_name}' (condition:{condition}) for update.")

            elif op_type == "delete":
                condition = operation.get("condition", {})

                query = session.query(TableClass)
                for key, value in condition.items():
                    query = query.filter(getattr(TableClass, key) == value)

                record = query.first()
                if record:
                    session.delete(record)
                    session.commit()
                    loggers["deleted"].info(f"Deleted record from '{table_name}' with condition {condition}")
                else:
                    loggers["skipped"].warning(f"Record not found in '{table_name}' (condition:{condition})  for delete.")
    except Exception as e:
        session.rollback()
        loggers["failures"].error(f"Unexpected error: {e}")
    finally:
        session.close()
        loggers["info"].info("Database session closed.")

# API call
url = "http://localhost:3000/file"
response = requests.get(url)
# loggers["info"].info(f"Response Status Code: {response.status_code}")

try:
    json_data = response.json()
    # loggers["info"].info(f"JSON data received: {json_data}")

    if "data" in json_data:
        execute_operations(json_data["data"])
    else:
        loggers["failures"].error("JSON response does not contain 'data' key.")
except requests.exceptions.JSONDecodeError:
    loggers["failures"].error(f"Invalid JSON response received: {response.text}")
