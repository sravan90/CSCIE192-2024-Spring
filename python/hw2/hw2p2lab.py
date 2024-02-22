import time
import pandas as pd
import pyarrow


def calculate_summary_metrics(input_file, summary_output_file_path, relative_output_file_path):
    try:
        # Read input CSV file
        df = pd.read_csv(input_file, skip_blank_lines=True, low_memory=False, na_values=['', 'NA', 'N/A', 'null', 'NaN'])

        df.dropna(subset=['passenger_count','fare_amount'], inplace = True)

        # Calculate summary metrics
        total_trips = len(df)

        total_passengers = df['passenger_count'].sum()
        average_fare_per_trip = df['fare_amount'].mean()
        median_fare_per_trip = df['fare_amount'].median()

        # Calculate relative metrics
        #mean_fare_by_payment_type = df.groupby('payment_type')['fare_amount'].mean()
        average_fare_per_trip_by_vendor = df.groupby('VendorID')['fare_amount'].mean()
        total_trip_distance_by_vendor = df.groupby('VendorID')['trip_distance'].sum()


        # Create summary output dataframe
        summary_df = pd.DataFrame({
            'total number of trips': [total_trips],
            'total passengers':[total_passengers],
            'average fare per trip':[average_fare_per_trip],
            'median fare per trip':[median_fare_per_trip]
        })


        # Create relative output dataframe
        relative_df = pd.DataFrame({
            'VendorID':average_fare_per_trip_by_vendor.index,
            'average fare trip by vendor': average_fare_per_trip_by_vendor.values,
            'total trip distance by vendor': total_trip_distance_by_vendor.values
        })





        # Write results to output CSV files
        summary_df.to_csv(summary_output_file_path, index=False)
        print("Summary metrics calculated and written to", summary_output_file_path)

        relative_df.to_csv(relative_output_file_path, index=False)
        print("Relative metrics calculated and written to", relative_output_file_path)

    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found.")


# Command-line execution
if __name__ == "__main__":
    import sys

    if len(sys.argv) < 3:
        print("Usage: python hw2p2.py input_file_path summary_output_file_path relative_output_file_path")
    else:
        input_file_path = sys.argv[1]
        summary_output_file_path = sys.argv[2]
        relative_output_file_path = sys.argv[3]
        # Measure the execution time of the function
        start_time = time.time()  # Record the current time

        calculate_summary_metrics(input_file_path, summary_output_file_path, relative_output_file_path )

        end_time = time.time()  # Record the current time again
        # Calculate the elapsed time
        elapsed_time = end_time - start_time
        print(f"Execution time: {elapsed_time} seconds")

calculate_summary_metrics('/Users/sravanspoorthy/PycharmProjects/CSCIE192-2024-Spring/python/hw2/input/taxi_tripdata.csv',
                         '/Users/sravanspoorthy/PycharmProjects/CSCIE192-2024-Spring/python/hw2/output/summary/summary.csv',
                          '/Users/sravanspoorthy/PycharmProjects/CSCIE192-2024-Spring/python/hw2/output/relative/relative.csv' )