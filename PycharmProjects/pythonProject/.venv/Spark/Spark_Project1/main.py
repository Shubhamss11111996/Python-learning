from src.create_tables import create_tables
from src.populate_data import populate_data
from src.etl_process import perform_etl

if __name__ == "__main__":
    print("Step 1: Create Tables")
    create_tables()

    print("\nStep 2: Populate Data")
    populate_data()

    print("\nStep 3: Perform ETL")
    perform_etl()
