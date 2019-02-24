from chalice import Chalice, Rate
import arrow
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
import requests
import boto3
import csv
import StringIO
import gzip

app = Chalice(app_name='nws_parser')
app.debug = True


s3 = boto3.resource('s3')


# @app.route('/')
# def index():
@app.schedule(Rate(1, unit=Rate.HOURS))
def index(event):
    url = "https://forecast.weather.gov/product.php?site=BTV&issuedby=BTV&product=HYD&format=CI&version=1"
    r = requests.get(url)
    html = r.text
    parsed_html = BeautifulSoup(html, 'html.parser')
    pre = parsed_html.body.find('pre', attrs={'class': 'glossaryProduct'})

    date = None
    snow_idx = 0
    snow_depth = None
    for line in pre.get_text().split('\n'):
        try:
            # date is of format "1000 AM EST Sun Feb 10 2019"
            date = arrow.get(line, "MMM D YYYY").datetime
            print "Date on NWS report is: %s" % date
            continue
        except arrow.parser.ParserError as e:
            # Most lines will throw value error, don't even log
            pass
        if line.startswith("Station") and "Snow" in line:
            snow_idx = line.index("Snow")
            continue
        if line.startswith("Mount Mansfield"):
            try:
                depth_str = line[snow_idx:snow_idx + 4]
                print depth_str
                snow_depth = int(depth_str)
                print snow_depth
            except ValueError:
                print "Error trying to convert string: %s to int" % depth_str

    # exit if date is none
    if not date:
        print "Unable to parse date from NWS report"
        return "No date found"

    # exit if we don't get a reported depth
    if snow_depth != 0 and not snow_depth:
        print "No depth in current text report"
        return "No new depth"

    # get current data from s3
    existing_data = s3.Object(bucket_name='matthewparrilla.com',
        key='newSnowDepth.csv')\
        .get()\
        .get('Body')\
        .read()
    uncompressed_file = gzip.GzipFile(fileobj=StringIO.StringIO(existing_data)).read()

    # csv string returns a list of lists [['9/1', 0], ...]
    data = [i for i in csv_string_to_list(uncompressed_file) if len(i) > 0]

    # figure out what cell to update
    date_string = date.strftime("%-m/%-d")
    print "Date to update: %s" % date_string
    date_index = data[0].index(date_string)
    year = date.year
    season = "%d-%d" % (
        (year, year + 1) if date.month > 7 else (year - 1, year))
    season_index = next((data.index(row) for row in data if row[0] == season), len(data))

    # --------------BEGIN EMAIL NOTIFICATION-----------------------------------
    try:
        SENDER = "Mansfield <support@bugbucket.io>"
        CHARSET = "UTF-8"
        client = boto3.client("ses")

        # Provide the contents of the email.
        client.send_email(
            Destination={
                'ToAddresses': ['matthew.parrilla@gmail.com'],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': "Date: %s Depth: %d" % (date_string, snow_depth)

                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': "mansfield snow depth",
                },
            },
            Source=SENDER,
        )
        print("Email sent!")

    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    # --------------END EMAIL NOTIFICATION-------------------------------------

    # check if we need to update data
    if data[season_index][date_index] == str(snow_depth):
        print "No new data. Exiting"
        return False
    else:
        # update copy of data
        data[season_index][date_index] = snow_depth
        print "Fresh data. Time for an update"

    # write list to csv string
    print "Writing data to CSV string"
    new_csv = StringIO.StringIO()
    writer = csv.writer(new_csv, quoting=csv.QUOTE_NONNUMERIC)
    writer.writerows(data)

    print "Compressing data"
    compressed_csv = StringIO.StringIO()
    with gzip.GzipFile(fileobj=compressed_csv, mode="w") as f:
        f.write(new_csv.getvalue())

    print "Pushing to S3"
    s3.Object("matthewparrilla.com", "newSnowDepth.csv").put(
        Body=compressed_csv.getvalue(),
        ContentEncoding='gzip',
        ACL="public-read")

    return "Great success!"


@app.route("/dummy")
def dummy():
    """Hack used to get chalice to generate proper IAM b/c of bug related to
       boto3.resource not triggering correct IAM policy:
       https://github.com/aws/chalice/issues/118#issuecomment-298490541
    """
    ddb = boto3.client("s3")
    try:
        ddb.get_object(Bucket="matthewparrilla.com")
        ddb.put_object(Bucket="matthewparrilla.com")
        ddb.put_object_acl(Bucket="matthewparrilla.com", ACL="public-read")
    except:
        print "move along"


def csv_string_to_list(csv_as_string):
    return list(csv.reader(csv_as_string.split("\n"), delimiter=","))
