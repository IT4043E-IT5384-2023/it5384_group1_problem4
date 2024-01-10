import pandas as pd
import os

# List of CSV files to merge
csv_files = ['BSC.csv', 'ETH.csv', 'Arbitrum.csv']

# Create an empty DataFrame to store the merged data
merged_data = pd.DataFrame()
current_dir = os.path.dirname(os.path.abspath(__file__))
# Iterate over each CSV file and merge its data into the DataFrame
for file in csv_files:
    file_path = os.path.join(current_dir, file)
    df = pd.read_csv(file_path)  # Read the CSV file into a DataFrame
    merged_data = pd.concat([merged_data, df], ignore_index=True)   # Append the DataFrame to the merged_data

# Write the merged data to a new CSV file
merged_data.to_csv('smart_contract.csv', index=False)