#configured with beeceptor mock api with the json body

#https://app.beeceptor.com/console/accelerator

import json
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

print(tables,'tables in db')

# if "users" in tables:
AutoBase = automap_base()

# Fix the prepare method based on SQLAlchemy version
# try:
AutoBase.prepare(autoload_with=engine)  # SQLAlchemy 2.0+
# except TypeError:
#     AutoBase.prepare(engine, reflect=True)  # SQLAlchemy <2.0

User = AutoBase.classes.get("users")
if not User:
    print("Error: Table 'users' not found in reflected database.")
# else:
    # print("Error: Table 'users' does not exist in the database.")

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
                        print(f" Created user: {data}")
                    except IntegrityError:
                        session.rollback()
                        print(f" User with id {data['id']} already exists. Skipping.")

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
                        print(f" Updated user: {data}")
                    else:
                        print(f" User with condition {condition} not found. Skipping update.")

                elif op_type == "delete":
                    condition = operation.get("condition", {})

                    query = session.query(User)
                    for key, value in condition.items():
                        query = query.filter(getattr(User, key) == value)

                    user = query.first()
                    if user:
                        session.delete(user)
                        session.commit()
                        print(f" Deleted user with condition {condition}")
                    else:
                        print(f" User with condition {condition} not found. Skipping delete.")

        print(" All operations executed successfully!")

    except Exception as e:
        session.rollback()
        print(f" Unexpected error: {e}")

    finally:
        session.close()
        print("Database session closed.")



#api call for the json
# url = "https://accelerator.free.beeceptor.com/file"
# response = requests.get(url)

# print(response,'response')
# # Print response data
# if response.status_code == 200:
#     print(response.json())  # Convert response to JSON
# else:
#     print("Error:", response.status_code)


# API call
url = "https://accelerator.free.beeceptor.com/file"
response = requests.get(url)

# Print response details
print(f"Response Status Code: {response.status_code}")

try:
    json_data = response.json()  # Convert response to JSON
    print(json_data, 'Response Data')

    # Pass the correct data
    if "data" in json_data:
        execute_operations(json_data["data"])
    else:
        print("Error: JSON response does not contain 'data' key.")
except requests.exceptions.JSONDecodeError:
    print("Invalid JSON response received:", response.text)
