# Car Sales ETL Pipeline

This is a Python-based Extract, Transform, Load (ETL) pipeline that automates the process of fetching car sales data from a remote source, cleaning or formatting it using Pandas, and loading it into a PostgreSQL database or DBeaver for further analysis.


# Data Features

The Extract pulls raw data directly form a remote Github CSV repository.

The Transformation cleans currency symbols ($) and removes commas from price data. This stage converts numeric strings to float types and handles missing values on columns like Make and Odometer. 

The Loading function effectively loads data into postgreSQL instance using SQLAlchemy and the Psycopg2 driver.


# Connection to Database
To configure the connection of the script to the database, the load function is updated with credentials to match the local database credentials like the USER, PASSWORD, HOST AND PORT. 