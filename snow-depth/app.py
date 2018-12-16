from chalice import Chalice
from datetime import datetime
import boto3
import requests
import csv
import re
import StringIO
import gzip
import copy

app = Chalice(app_name='snow-depth')
app.debug = True

s3 = boto3.resource('s3')

# generate season key, needed in multiple places
date = datetime.now()
y = date.year
m = date.month
season_key = "%d-%d" % (y if m > 8 else y - 1, y + 1 if m > 8 else y)


# @app.route('/')
# def index():
@app.schedule('cron(0 12-18/2 * 1,2,3,4,5,9,10,11 ? *)')
def index(event):
    existing_data = s3.Object(bucket_name='matthewparrilla.com', key='snowDepth.csv')\
        .get()\
        .get('Body')\
        .read()
    uncompressed_file = gzip.GzipFile(fileobj=StringIO.StringIO(existing_data)).read()
    s3_data = csv_string_to_list(uncompressed_file)  # list of lists [['9/1', 0], ...]
    current_season_on_uvm = get_year_from_uvm()

    # make copy of original list so we can compare after we modify
    data = copy.deepcopy(s3_data)

    print "Updating data"
    season_idx = next(i for i, row in enumerate(data) if row[0] == season_key)
    for i, item in enumerate(data[season_idx]):
        key = data[0][i]  # first row of data is headers
        data[season_idx][i] = current_season_on_uvm.get(key, '')

    if data == s3_data:
        print "No new data. Exiting"
        return False

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
    s3.Object("matthewparrilla.com", "snowDepth.csv").put(
        Body=compressed_csv.getvalue(),
        ContentEncoding='gzip',
        ACL="public-read")
    return True


def csv_string_to_list(csv_as_string):
    return list(csv.reader(csv_as_string.split("\n"), delimiter=","))


def get_year_from_uvm():

    print "Requesting data from UVM"
    # This might break on Jan 1
    url = ("http://waw.w3.uvm.edu/empactdata/gendateplot.php?"
           "table=SummitStation&title=Mount+Mansfield+Summit+Station&"
           "xskip=7&xparam=Date&yparam=Depth&year%s=%d&csv=1&totals=0"
          ) % ("%5B%5D", y if m > 8 else y - 1)
    r = requests.get(url)

    # data is returned inside of a <pre> element
    content = r.content.replace("<pre>", "").replace("</pre>", "")
    depth_dict = {"year": season_key}

    # filter out first row (name of season) and empty rows
    just_data = [i for i in csv_string_to_list(content)
        if len(i) > 1 and re.match("^\d{4}-", i[0])]

    # TODO: munge data here
    for date, depth in just_data:
        _, month, day = date.split('-')
        depth_dict["%d/%d" % (int(month), int(day))] = depth
    return depth_dict


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
