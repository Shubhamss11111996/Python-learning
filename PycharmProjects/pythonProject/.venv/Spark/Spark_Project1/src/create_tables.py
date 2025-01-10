from sqlalchemy import create_engine, text
import json


def load_credentials():
    with open("data/db_credentials.json", "r") as file:
        return json.load(file)


def create_tables():
    credentials = load_credentials()
    connection_url = f"oracle+cx_oracle://{credentials['ORACLE_USER']}:{credentials['ORACLE_PASSWORD']}@{credentials['ORACLE_HOST']}:{credentials['ORACLE_PORT']}/?service_name={credentials['ORACLE_SERVICE_NAME']}"

    engine = create_engine(connection_url)

    create_sales_query = text("""
    CREATE TABLE sales_data (
        product_id NUMBER,
        sales_amount FLOAT,
        timestamp DATE,
        region VARCHAR2(50)
    )
    """)

    create_transformed_query = text("""
    CREATE TABLE transformed_sales_data (
        region VARCHAR2(50),
        total_sales FLOAT
    )
    """)

    with engine.connect() as conn:
        # Drop existing tables if they exist
        try:
            conn.execute(text("DROP TABLE sales_data"))
            print("Table `sales_data` dropped.")
        except Exception as e:
            print(f"No need to drop `sales_data`: {e}")

        try:
            conn.execute(create_sales_query)
            print("Table `sales_data` created.")
        except Exception as e:
            print(f"Error creating `sales_data`: {e}")

        try:
            conn.execute(text("DROP TABLE transformed_sales_data"))
            print("Table `transformed_sales_data` dropped.")
        except Exception as e:
            print(f"No need to drop `transformed_sales_data`: {e}")

        try:
            conn.execute(create_transformed_query)
            print("Table `transformed_sales_data` created.")
        except Exception as e:
            print(f"Error creating `transformed_sales_data`: {e}")
