import gzip
import boto3
import shutil
import argparse

boto3.setup_default_session(profile_name="personal")
s3 = boto3.resource('s3')


def update_csv_on_s3(new_file, file_to_update):
    with open(new_file, 'rb') as f_in:
        gzipped_new_file = "{}.gz".format(new_file)
        with gzip.open(gzipped_new_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    print("Pushing to S3")
    s3.Object("matthewparrilla.com", file_to_update).put(
        Body=open(gzipped_new_file, 'rb'),
        ContentEncoding='gzip',
        ACL="public-read")


parser = argparse.ArgumentParser()
parser.add_argument("--new_file", required=True, help="File to push to S3")
parser.add_argument("--update_name", required=True, help="File to update on S3")
args = parser.parse_args()

if __name__ == "__main__":
    update_csv_on_s3(args.new_file, args.update_name)
