import boto3
import gzip
import io
from datetime import datetime

# Initialize the S3 client
s3 = boto3.client('s3')
bucket_name = 'matthewparrilla.com'
key = 'snowDepth.csv'

def download_snow_depth():
    # Get the object from S3
    response = s3.get_object(Bucket=bucket_name, Key=key)
    compressed_data = response['Body'].read()
    
    # Decompress the data
    uncompressed_data = gzip.GzipFile(fileobj=io.BytesIO(compressed_data)).read().decode('utf-8')
    
    # Generate the filename with a datestamp
    datestamp = datetime.now().strftime('%Y%m%d')
    filename = f'snowDepth-{datestamp}.csv.bak'
    
    # Write the uncompressed data to a local file
    with open(filename, 'w') as f:
        f.write(uncompressed_data)
    
    print(f"Downloaded {filename}")

if __name__ == "__main__":
    # Execute the download function if the script is run directly
    download_snow_depth()
