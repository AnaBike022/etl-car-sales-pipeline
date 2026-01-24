# import libraries

import pandas as pd
import numpy as np

# extract data

def extract(url):
    """
    extract data from a CSV file url
    """

    df = pd.read_csv(url)
    print("data extracted successfully")
    return df

#transform data

def transform(df):
    """
    clean and transform data
    """
    print("transform completed")

    # remove duplicate rows
    df = df.drop_duplicates()

    # drop rows where price is missing
    df = df.dropna(subset=["Price"])

    # handle odometer and doors missing vaues with mean
    df["Odometer (KM)"] = df["Odometer (KM)"].fillna(df["Odometer (KM)"].mean())
    df["Doors"] = df["Doors"].fillna(df["Doors"].mean())

    # handle missing string values
    df["Make"] = df["Make"].fillna("Unknown")
    df["Colour"] = df["Colour"].fillna("Unknown")

    print("Data transformation completed.")
    return df


    # CSV files are human readable but not efficient for big data storage like Parquet files.
    # They take up more space and are slower to read for big computers.
    # Parquet files are more efficient for storing data though not as hman readable as CSVs. 
    # It is compressed and much faster for big computers to read, though humans can't read it by just opening it in Notepad.



# load data to CSV and parquet files

def load(df, csv_carsales, parquet_carsales):
    """
    load data to csv and parquet files
    """
    
    df.to_csv(csv_carsales, index = False)
    #df.to_parquet(parquet_carsales, index = False)
    df.to_parquet("cleaned_car_sales.parquet", engine="fastparquet", index=False)

    print("data loaded successfully")

    return


# Run pipeline

def run_pipeline(input_path, csv_output_path, parquet_output_path):
    """
    run ETL pipeline
    """

    df = extract(input_path)
    df = transform(df)
    load(df, csv_output_path, parquet_output_path)
    print("ETL pipeline run successfully")
    return df


pip install pyarrow

pip install fastparquet


if __name__ == "__main__":
    data_url = "https://raw.githubusercontent.com/mrdbourke/zero-to-mastery-ml/master/data/car-sales-extended-missing-data.csv"

    run_pipeline(
        data_url,
        "cleaned_car_sales.csv",
        "cleaned_car_sales.parquet"
    )


cleaned_car_sales = pd.read_csv("cleaned_car_sales.csv")
print(cleaned_car_sales.head())


cleaned_car_sales = pd.read_parquet("cleaned_car_sales.parquet")
print(cleaned_car_sales.head()) 

df = pd.read_csv("cleaned_car_sales.csv")
print(df.head())
df.head()

