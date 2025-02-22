import boto3
import gzip
import io

s3 = boto3.resource('s3')
bucket_name = 'matthewparrilla.com'
key = 'snowDepth.csv'

def upload_snow_depth():
    with open('snowDepth.csv', 'r') as f:
        data = f.read()
    
    compressed_data = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_data, mode="w") as gz:
        gz.write(data.encode('utf-8'))
    
    s3.Object(bucket_name, key).put(
        Body=compressed_data.getvalue(),
        ContentEncoding='gzip',
        ACL='public-read'
    )
    
    print("Uploaded snowDepth.csv")

if __name__ == "__main__":
    upload_snow_depth()
