#Single table with loggers


import json
import os
from datetime import datetime
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import IntegrityError
import requests

# Database connection URL
DATABASE_URL = "postgresql://postgres:Jio123@localhost:5433/postgres"

# Create engine & session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

# Reflect database tables
inspector = inspect(engine)
tables = inspector.get_table_names()

print(tables, 'tables in db')

AutoBase = automap_base()
AutoBase.prepare(autoload_with=engine)

User = AutoBase.classes.get("users")
if not User:
    print("Error: Table 'users' not found in reflected database.")

# def log_to_file(filename, data):
#     timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
#     log_entry = {"timestamp": timestamp, "data": data}
#     if os.path.exists(filename):
#         with open(filename, "r+") as file:
#             try:
#                 existing_data = json.load(file)
#             except json.JSONDecodeError:
#                 existing_data = []
#             existing_data.append(log_entry)
#             file.seek(0)
#             json.dump(existing_data, file, indent=4)
#     else:
#         with open(filename, "w") as file:
#             json.dump([log_entry], file, indent=4)

def log_to_file(filename, data):
    log_dir = "loggers"
    os.makedirs(log_dir, exist_ok=True)  # Ensure 'loggers' folder exists
    file_path = os.path.join(log_dir, filename)  # Construct full file path

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = {"timestamp": timestamp, "data": data }

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

def execute_operations(operations):
    try:
        for operation in operations:
            table_name = operation.get("table")
            op_type = operation.get("operation")
            
            if table_name == "users":
                if op_type == "create":
                    data = operation.get("data", {})
                    try:
                        new_user = User(**data)
                        session.add(new_user)
                        session.commit()
                        print(f"Created user: {data}")
                        log_to_file("created_records.json", data)
                    except IntegrityError:
                        session.rollback()
                        print(f"User with id {data['id']} already exists. Skipping.")
                        log_to_file("skipped_operations.json", data)

                elif op_type == "update":
                    condition = operation.get("condition", {})
                    data = operation.get("data", {})

                    query = session.query(User)
                    for key, value in condition.items():
                        query = query.filter(getattr(User, key) == value)

                    user = query.first()
                    if user:
                        for key, value in data.items():
                            setattr(user, key, value)
                        session.commit()
                        print(f"Updated user: {data}")
                        log_to_file("updated_records.json", data)
                    else:
                        print(f"User with condition {condition} not found. Skipping update.")
                        log_to_file("skipped_operations.json", condition)

                elif op_type == "delete":
                    condition = operation.get("condition", {})

                    query = session.query(User)
                    for key, value in condition.items():
                        query = query.filter(getattr(User, key) == value)

                    user = query.first()
                    if user:
                        session.delete(user)
                        session.commit()
                        print(f"Deleted user with condition {condition}")
                        log_to_file("deleted_records.json", condition)
                    else:
                        print(f"User with condition {condition} not found. Skipping delete.")
                        log_to_file("skipped_operations.json", condition)

        print("All operations executed successfully!")
    except Exception as e:
        session.rollback()
        print(f"Unexpected error: {e}")
        log_to_file("failures.json", str(e))
        log_to_file("failed_operations.json", operations)
    finally:
        session.close()
        print("Database session closed.")

# API call
url = "http://localhost:3000/file"
response = requests.get(url)

# Print response details
print(f"Response Status Code: {response.status_code}")

try:
    json_data = response.json()
    print(json_data, 'Response Data')
    
    if "data" in json_data:
        execute_operations(json_data["data"])
    else:
        print("Error: JSON response does not contain 'data' key.")
except requests.exceptions.JSONDecodeError:
    print("Invalid JSON response received:", response.text)