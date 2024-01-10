
import psycopg2
import csv

def export_data_to_csv(query, output_file, batch_size):
    connect_str = "user=student_token_transfer host=34.126.75.56 password=svbk_2023 port=5432 dbname=postgres"

    # Connect to the PostgreSQL database
    # conn = psycopg2.connect(
    #         host=34.126.75.56,
    #         database=chain_0x1,
    #         user=student_token_transfer,
    #         password=svbk_2023,
    #         port=5432
    # )
    conn = psycopg2.connect(connect_str)
    # Create a cursor object
    cursor = conn.cursor()

    try:
        # Initialize the offset
        offset = 0

        # Open the output CSV file in append mode
        with open(output_file, 'a', newline='') as csv_file:
            writer = csv.writer(csv_file)
            rows_count = 0
            while True and rows_count < 2000000:
                # Build the query with LIMIT and OFFSET
                batch_query = query + f" LIMIT {batch_size} OFFSET {offset}"

                # Execute the batch query
                cursor.execute(batch_query)

                # Get the column names
                column_names = [desc[0] for desc in cursor.description]
                # Write the column headers
                writer.writerow(column_names)

                # Fetch the rows
                rows = cursor.fetchall()

                # If no more rows are returned, exit the loop
                if not rows:
                    break


                # Write the rows to the CSV file
                writer.writerows(rows)

                # Increment the offset
                offset += batch_size

                rows_count += batch_size

        print(f"Data exported successfully to {output_file}")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")

    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

# Example usage
dict_chain = {
    'chain_0x1' : 'ETH',
    'chain_0x38' : 'BSC',
    'chain_0xa4b1' : 'Arbitrum'}
for chain in dict_chain:
    query = "SELECT token_transfer.* FROM " + chain + ".token_transfer INNER JOIN " + chain +".smart_contract ON token_transfer.to_address = smart_contract.contract_address"
    output_file = 'T/' + dict_chain[chain] + '.csv'
    batch_size = 100000  # Set the desired batch size
    export_data_to_csv(query, output_file, batch_size)