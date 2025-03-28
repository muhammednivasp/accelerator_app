

# import json
# from sqlalchemy import create_engine, Column, Integer, String, inspect
# from sqlalchemy.orm import declarative_base, sessionmaker
# from sqlalchemy.ext.automap import automap_base

# # Database connection URL
# DATABASE_URL = "postgresql://postgres:Jio123@localhost:5433/postgres"

# # Create engine & session
# engine = create_engine(DATABASE_URL)
# Session = sessionmaker(bind=engine)
# session = Session()
# Base = declarative_base()

# # Inspect the database to check for existing tables
# inspector = inspect(engine)
# tables = inspector.get_table_names()
# print(f"Tables in the database: {tables}")

# # If 'users' table exists, reflect it dynamically
# if "users" in tables:
#     AutoBase = automap_base()
#     AutoBase.prepare(engine, reflect=True)

#     if "users" in AutoBase.classes:
#         User = AutoBase.classes["users"]
#         print("User model reflected dynamically.")
#     else:
#         print("Error: Table 'users' not found in reflected database.")
# else:
#     print("Error: Table 'users' does not exist in the database.")


# # Function to execute CRUD operations dynamically
# def execute_operations(json_file):
#     try:
#         # Read the JSON file
#         with open(json_file, "r") as file:
#             operations = json.load(file)

#         for operation in operations:
#             table_name = operation.get("table")
#             op_type = operation.get("operation")

#             if table_name == "users":  # Handling 'users' table
#                 if op_type == "create":
#                     data = operation.get("data", {})
#                     new_user = User(**data)
#                     session.add(new_user)
#                     print(f"Created user: {data}")

#                 elif op_type == "update":
#                     condition = operation.get("condition", {})
#                     data = operation.get("data", {})

#                     query = session.query(User)
#                     for key, value in condition.items():
#                         query = query.filter(getattr(User, key) == value)

#                     user = query.first()
#                     if user:
#                         for key, value in data.items():
#                             setattr(user, key, value)
#                         print(f"Updated user: {data}")
#                     else:
#                         print(f"User with condition {condition} not found.")

#                 elif op_type == "delete":
#                     condition = operation.get("condition", {})

#                     query = session.query(User)
#                     for key, value in condition.items():
#                         query = query.filter(getattr(User, key) == value)

#                     user = query.first()
#                     if user:
#                         session.delete(user)
#                         print(f"Deleted user with condition {condition}")
#                     else:
#                         print(f"User with condition {condition} not found.")

#         session.commit()
#         print("All operations executed successfully!")

#     except Exception as e:
#         session.rollback()
#         print(f"Error executing operations: {e}")


# # Execute the operations using the JSON file
# execute_operations("myfile.json")

# # Close session
# session.close()
# print("Database operations completed successfully!")



#================
#configured with local json file

import json
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.exc import IntegrityError

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


def execute_operations(json_file):
    try:
        # Read JSON file
        with open(json_file, "r") as file:
            operations = json.load(file)

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


# Execute operations
execute_operations("myfile.json")

