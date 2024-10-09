# data_pipeline.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

def generate_synthetic_data(num_properties=1000):
    np.random.seed(42)  # for reproducibility
    
    data = {
        'property_id': range(1, num_properties + 1),
        'address': [f"{np.random.randint(100, 9999)} Main St, City {i}" for i in range(num_properties)],
        'sqft': np.random.randint(800, 4000, num_properties),
        'bedrooms': np.random.randint(1, 6, num_properties),
        'bathrooms': np.random.randint(1, 4, num_properties),
        'year_built': np.random.randint(1950, 2023, num_properties),
    }
    
    today = datetime.now()
    for i in range(12):
        month = today - timedelta(days=30*i)
        base_price = np.random.randint(100000, 1000000, num_properties)
        monthly_change = np.random.normal(0, 0.02, num_properties)
        data[f'price_{month.strftime("%Y_%m")}'] = base_price * (1 + monthly_change)
    
    df = pd.DataFrame(data)
    df['zestimate'] = df[f'price_{today.strftime("%Y_%m")}'] * np.random.normal(1, 0.05, num_properties)
    
    return df

def process_data(df):
    price_columns = [col for col in df.columns if col.startswith('price_')]
    for column in price_columns:
        df = df.withColumn(column, col(column).cast("double"))
        df = df.withColumn(column.replace('price_', 'date_'), to_date(column.replace('price_', ''), 'yyyy_MM'))
    return df

def main():
    # Generate synthetic data
    synthetic_data = generate_synthetic_data()
    synthetic_data.to_csv('synthetic_real_estate_data.csv', index=False)
    print("Synthetic data saved to 'synthetic_real_estate_data.csv'")

    # Initialize Spark session
    spark = SparkSession.builder.appName("RealEstateDataPipeline").getOrCreate()

    # Read synthetic data
    df = spark.read.csv('synthetic_real_estate_data.csv', header=True, inferSchema=True)

    # Process data
    processed_df = process_data(df)

    # Save processed data
    processed_df.toPandas().to_csv('processed_real_estate_data.csv', index=False)
    print("Processed data saved to 'processed_real_estate_data.csv'")

if __name__ == "__main__":
    main()