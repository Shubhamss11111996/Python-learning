import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import random
import json


def load_credentials():
    with open("data/db_credentials.json", "r") as file:
        return json.load(file)


def populate_data():
    credentials = load_credentials()
    connection_url = f"oracle+cx_oracle://{credentials['ORACLE_USER']}:{credentials['ORACLE_PASSWORD']}@{credentials['ORACLE_HOST']}:{credentials['ORACLE_PORT']}/?service_name={credentials['ORACLE_SERVICE_NAME']}"

    engine = create_engine(connection_url)

    # Generate mock data
    product_ids = [101, 102, 103, 104, 105]
    regions = ["North", "South", "East", "West"]
    start_date = datetime(2023, 1, 1)
    data = [
        (random.choice(product_ids),
         round(random.uniform(10.0, 500.0), 2),
         start_date + timedelta(days=random.randint(0, 365)),
         random.choice(regions))
        for _ in range(1000)
    ]
    df = pd.DataFrame(data, columns=["product_id", "sales_amount", "timestamp", "region"])

    # Insert data
    df.to_sql("sales_data", con=engine, if_exists="append", index=False)
    print("Mock data populated.")
