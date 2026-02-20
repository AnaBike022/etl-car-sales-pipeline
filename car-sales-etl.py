
# import libraries
from py_compile import main

import pandas as pd
from sqlalchemy import create_engine


# extract data from csv file
def extract():
    df = pd.read_csv("https://raw.githubusercontent.com/mrdbourke/zero-to-mastery-ml/master/data/car-sales-extended-missing-data.csv")
    return df


# transforming the data
# 2. TRANSFORM: Clean the data
def transform(df):
    # This cleans the 'Price' column: removes '$', commas, and converts to numbers
    # We also handle missing values (NaNs) so the database doesn't complain
    if 'Price' in df.columns:
        df['Price'] = df['Price'].astype(str).str.replace(r'[\$,]', '', regex=True)
        df['Price'] = pd.to_numeric(df['Price'], errors='coerce')
    
    # Fill missing values in other columns with 'Unknown' or 0
    df.fillna({'Make': 'Missing', 'Colour': 'Missing', 'Odometer (KM)': 0}, inplace=True)
    
    print("Data transformation complete.")
    return df


# saving the data to a parquet file

def load(df):
    
    USER = "postgres"
    PASSWORD = "postgre" 
    HOST = "localhost"
    PORT = "5432"
    DATABASE = "postgres"
    TABLE = "car_sales"
    SCHEMA = "public"

    # Create a connection string
    connection_string = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

    # Create a SQLAlchemy engine
    db_engine = create_engine(connection_string)

    # Save the DataFrame to PostgreSQL
    df.to_sql(TABLE, db_engine, schema=SCHEMA, if_exists="replace", index=False)


    # Run Pipeline
def run_pipeline():
    data = extract()
    cleaned_data = transform(data)
    load(cleaned_data)

# This part tells Python to actually START the pipeline
if __name__ == "__main__":
    run_pipeline()

    


