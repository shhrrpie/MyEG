import sqlite3  
import datetime
import pandas as pd
from datetime import date
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def process_log_data():
    try:
        # Getting date to read input file
        today = date.today()
        yesterday = today - datetime.timedelta(days=1)
        yesterday_date = yesterday.strftime("%Y%m%d")

        # To store data in the list
        data = []

        # Input file location
        log_data_path = f'input_file/apache_access_log_{yesterday_date}.csv'

        # Request_by_myeg file location
        path_output_page_views_by_url= f'request_by_myeg/page_views/df_page_views_by_url_{yesterday_date}.txt'
        path_output_traffic_source = f'request_by_myeg/traffic_source/df_traffic_source_{yesterday_date}.txt'

        # Output file location
        output_db_path = 'output_file/traffic.db'

        # Read apache access log & identify data location
        with open(log_data_path, 'r') as file:
            for line in file:
                parts = line.split()
                ip = parts[0]
                timestamp = ' '.join(parts[3:5]).strip('[]')
                request = ' '.join(parts[5:8]).strip('"')
                method, url, protocol = request.split() if len(request.split()) == 3 else (None, None, None)
                status = int(parts[8]) if parts[8].isdigit() else None
                response_size = int(parts[9]) 
                country = parts[13]
                request_time_microseconds = int(parts[-3])
                page_views = int(parts[-1])
                data.append([ip, timestamp, method, url, protocol, status, response_size, country, request_time_microseconds, page_views])

        # Store it in dataframe
        df = pd.DataFrame(data, columns=['ip', 'timestamp', 'method', 'url', 'protocol', 'status', 'response_size', 'country', 'request_time_microseconds', 'page_views'])

        # Duplicate column to extract utm_source
        df['utm_source'] = df['url']

        # Dataframe with utm_source
        df_with_utm = df[df['utm_source'].str.contains('=')].copy()
        df_with_utm.loc[:, 'utm_source'] = df_with_utm['utm_source'].str.split('=', expand=True)[1]
        df_with_utm['utm_source'] = df_with_utm['utm_source'].str.replace('null', 'direct', regex=True)

        # Dataframe without utm_source
        df_without_utm = df[~df['utm_source'].str.contains('=')].copy()  
        df_without_utm.loc[:, 'utm_source'] = "null" 

        # Merge both dataframe
        combined_df = pd.concat([df_with_utm, df_without_utm], axis=0)

        # To remove extra space at the end
        combined_df['timestamp'] = combined_df['timestamp'].str.strip()

        # Convert the datetime column to datetime objects
        combined_df['timestamp'] = pd.to_datetime(combined_df['timestamp'], format='%d/%b/%Y:%H:%M:%S')

        # Convert datetime objects to desired string format
        combined_df['timestamp'] = combined_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        #######################REQUEST BY MYEG#######################
        
        # Group the DataFrame by 'url' and sum the 'page_views' for each group
        df_page_views_by_url = combined_df.groupby('url')['page_views'].sum().reset_index()

        # Rename the columns for clarity
        df_page_views_by_url.columns = ['url', 'total_page_views']
        
        # Export df_page_views_by_url to txt
        df_page_views_by_url.to_csv(path_output_page_views_by_url, sep='\t', index=False)
        
        # Group the DataFrame by 'url' and sum the 'page_views' for each group
        df_traffic_source = combined_df.groupby('utm_source')['page_views'].sum().reset_index()

        # Rename the columns for clarity
        df_traffic_source.columns = ['utm_source', 'total_page_views']

        # Export to txt with tab separator, excluding index and header
        df_traffic_source.to_csv(path_output_traffic_source, sep='\t', index=False)

        #######################REQUEST BY MYEG#######################

        # Save data into traffic.db
        try:
            # Create a connection to the SQLite database
            conn = sqlite3.connect(output_db_path)

            # Save the DataFrame to the SQLite database
            combined_df.to_sql('website_traffic', conn, if_exists='append', index=False)
            
            # Print succeed statement
            print(f"Succeed: DB data saved to {output_db_path}")

        except Exception as e:        
            # Print failed statement
            print(f"Failed: {e}")
        
        finally:
            # Close the connection
            conn.close()

    except Exception as e:
        # Print failed statement
        print(f"Failed: {e}")

# Define the default_args dictionary
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'process_apache_log',  # DAG name
    default_args=default_args,
    description='A simple DAG to process Apache logs daily',
    schedule_interval='1 0 * * *',  # Run daily at 12:01 AM
)

# Define the task
process_log_data_task = PythonOperator(
    task_id='process_log_data',
    python_callable=process_log_data,
    dag=dag,
)

# Set the task
process_log_data_task
