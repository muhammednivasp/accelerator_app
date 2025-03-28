#loggers with predefined tables

import json
import os
from datetime import datetime
from sqlalchemy import create_engine, inspect, Column, Integer, String, Boolean, Float, DateTime, MetaData, Table, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
import requests

# Database connection URL
DATABASE_URL = "postgresql://postgres:Jio123@localhost:5433/postgres"

# Create engine & session
engine = create_engine(DATABASE_URL, future=True)  # Ensure future compatibility with SQLAlchemy 2.x
Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
session = Session()

# Reflect existing database tables
metadata = MetaData()
metadata.reflect(bind=engine)

AutoBase = automap_base(metadata=metadata)
AutoBase.prepare(autoload_with=engine)

# Logger function
def log_to_file(filename, data, table_name=None):
    log_dir = "loggers"
    os.makedirs(log_dir, exist_ok=True)
    file_path = os.path.join(log_dir, filename)

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {"timestamp": timestamp, "data": data}
    if table_name:
        log_entry["table_name"] = table_name 

    if os.path.exists(file_path):
        with open(file_path, "r+", encoding="utf-8") as file:
            try:
                existing_data = json.load(file)
            except json.JSONDecodeError:
                existing_data = []
            existing_data.append(log_entry)
            file.seek(0)
            json.dump(existing_data, file, indent=4)
    else:
        with open(file_path, "w", encoding="utf-8") as file:
            json.dump([log_entry], file, indent=4)

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
#         print(f"Creating table: {table_name}")

#         columns = [Column("id", Integer, primary_key=True, autoincrement=True)]
        
#         for key, value in data.items():
#             if key != "id":
#                 col_type = infer_sqlalchemy_type(value)
#                 columns.append(Column(key, col_type))

#         new_table = Table(table_name, metadata, *columns)
#         new_table.create(engine)
        
#         # Reflect the updated schema
#         metadata.reflect(bind=engine)
#         AutoBase.prepare(autoload_with=engine)

#         return AutoBase.classes.get(table_name)
    
#     return AutoBase.classes.get(table_name)

# Function to execute operations dynamically
def execute_operations(operations):
    try:
        for operation in operations:
            table_name = operation.get("table")
            op_type = operation.get("operation")
            data = operation.get("data", {})

            if not table_name or not op_type:
                print("Error: Missing table name or operation type. Skipping...")
                log_to_file("skipped_operations.json", operation, table_name)
                continue

            TableClass = AutoBase.classes.get(table_name) 
            # or create_table_if_not_exists(table_name, data)

            if not TableClass:
                print(f"Failed to create or retrieve table '{table_name}'. Skipping operation...")
                log_to_file("skipped_operations.json", operation, table_name)
                continue

            valid_columns = {col.name for col in TableClass.__table__.columns}
            # incoming_columns = set(data.keys())

            # # Check if any new column is present in data but not in table
            # new_columns = incoming_columns - valid_columns
            # if new_columns:
            #     print(f"Error: New columns detected in '{table_name}': {new_columns}. Rolling back operation.")
            #     log_to_file("failures.json", {"error": "Unexpected columns", "new_columns": list(new_columns), "data": data}, table_name)
            #     session.rollback()
            #     continue  # Skip this operation

            filtered_data = {k: v for k, v in data.items() if k in valid_columns}

            if op_type == "create":
                try:
                    new_record = TableClass(**filtered_data)
                    session.add(new_record)
                    session.commit()
                    print(f"Created record in '{table_name}': {filtered_data}")
                    log_to_file("created_records.json", filtered_data, table_name)
                except TypeError as e:
                    print(f"Error: {e}. Skipping invalid data for table '{table_name}'.")
                    log_to_file("skipped_operations.json", {"error": str(e), "data": data}, table_name)
                except IntegrityError:
                    session.rollback()
                    print(f"Record with the same ID already exists in '{table_name}'. Skipping.")
                    log_to_file("skipped_operations.json", data, table_name)

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
                    print(f"Updated record in '{table_name}': {filtered_data}")
                    log_to_file("updated_records.json", filtered_data, table_name)
                else:
                    print(f"Record with condition {condition} not found in '{table_name}'. Skipping update.")
                    log_to_file("skipped_operations.json", condition, table_name)

            elif op_type == "delete":
                condition = operation.get("condition", {})

                query = session.query(TableClass)
                for key, value in condition.items():
                    query = query.filter(getattr(TableClass, key) == value)

                record = query.first()
                if record:
                    session.delete(record)
                    session.commit()
                    print(f"Deleted record from '{table_name}' with condition {condition}")
                    log_to_file("deleted_records.json", condition, table_name)
                else:
                    print(f"Record with condition {condition} not found in '{table_name}'. Skipping delete.")
                    log_to_file("skipped_operations.json", condition, table_name)
    except Exception as e:
        session.rollback()
        print(f"Unexpected error: {e}")
        log_to_file("failures.json", str(e))
    finally:
        session.close()
        print("Database session closed.")

# API call
url = "http://localhost:3000/file"
response = requests.get(url)
print(f"Response Status Code: {response.status_code}")

try:
    json_data = response.json()
    if "data" in json_data:
        execute_operations(json_data["data"])
    else:
        print("Error: JSON response does not contain 'data' key.")
except requests.exceptions.JSONDecodeError:
    print("Invalid JSON response received:", response.text)
