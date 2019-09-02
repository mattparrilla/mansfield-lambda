from __future__ import print_function
from chalice import Chalice, Rate
from bs4 import BeautifulSoup
from datetime import datetime
import arrow
import requests
import boto3
import csv
import StringIO
import gzip

app = Chalice(app_name='nws_parser')
app.debug = True

TEMPERATURE_CSV = "mansfield_temperature.csv"
OBSERVATIONS_CSV = "mansfield_observations.csv"
SNOW_DEPTH_CSV = "snowDepth.csv"


s3 = boto3.resource('s3')


def update_observation(data):
    print("Updating current observation data")

    observation_data = s3.Object(bucket_name='matthewparrilla.com',
        key=OBSERVATIONS_CSV).get().get('Body').read().decode("ascii")

    # last observation is at -2 b/c each observation ends w/ newline
    last_observation = observation_data.split('\n')[-2]
    timestamp = arrow.get(last_observation.split(',')[0])

    for observation in data:
        if arrow.get(observation["timestamp"]) > timestamp:
            observation_data += "{},{},{},{},{}\n".format(
                observation["timestamp"].isoformat(),
                observation["temperature"],
                observation["direction"],
                observation["wind"],
                observation["gust"])

    s3.Object("matthewparrilla.com", OBSERVATIONS_CSV).put(
        Body=observation_data)


def update_temp(max_temp, min_temp, cur_temp, date):
    if max_temp is None:
        print("No temps in current text report")
        return
    print("Updating temperature data")

    # TODO: this puts whole CSV into memory, probably shouldn't do that,
    # should just append since we aren't even doing anything with it but appending
    data = s3.Object(bucket_name='matthewparrilla.com',
        key=TEMPERATURE_CSV).get().get('Body').read().decode("ascii")
    data += "{},{},{},{},{}\n".format(datetime.now().isoformat(),
        date,
        max_temp,
        min_temp,
        cur_temp)

    s3.Object("matthewparrilla.com", TEMPERATURE_CSV).put(
        Body=data)


def update_snow_depth(snow_depth, date):
    # exit if we don't get a reported depth
    if snow_depth is None:
        print("No depth in current text report")
        return

    # get current data from s3
    existing_data = s3.Object(bucket_name='matthewparrilla.com',
        key=SNOW_DEPTH_CSV)\
        .get()\
        .get('Body')\
        .read()
    uncompressed_file = gzip.GzipFile(fileobj=StringIO.StringIO(existing_data)).read()

    # csv string returns a list of lists [['9/1', 0], ...]
    data = [i for i in csv_string_to_list(uncompressed_file) if len(i) > 0]

    # figure out what cell to update
    date_string = date.strftime("%-m/%-d")
    print("Date to update: {}".format(date_string))
    date_index = data[0].index(date_string)
    year = date.year
    season = "%d-%d" % (
        (year, year + 1) if date.month > 7 else (year - 1, year))
    season_index = next((data.index(row) for row in data if row[0] == season), len(data))

    # check if we need to update data
    try:
        if data[season_index][date_index] == str(snow_depth):
            print("No new snow depth data. Exiting")
            return False
        else:
            # update copy of data
            data[season_index][date_index] = snow_depth
            print("Fresh data. Time for an update")

    # this is our first entry for the year
    except IndexError:
        print("season_index: {}".format(season_index))
        print("date_index: {}".format(date_index))
        print(data[-1])
        data[season_index] = [snow_depth]

    # write list to csv string
    print("Writing data to CSV string")
    new_csv = StringIO.StringIO()
    writer = csv.writer(new_csv, quoting=csv.QUOTE_NONNUMERIC)
    writer.writerows(data)

    print("Compressing data")
    compressed_csv = StringIO.StringIO()
    with gzip.GzipFile(fileobj=compressed_csv, mode="w") as f:
        f.write(new_csv.getvalue())

    print("Pushing to S3")
    s3.Object("matthewparrilla.com", SNOW_DEPTH_CSV).put(
        Body=compressed_csv.getvalue(),
        ContentEncoding='gzip',
        ACL="public-read")


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
    temp_idx = 0
    max_temp = None
    min_temp = None
    cur_temp = None
    snow_depth = None
    for line in pre.get_text().split('\n'):
        try:
            # date is of format "1000 AM EST Sun Feb 10 2019"
            date = arrow.get(line, "MMM D YYYY").datetime
            print("Date on NWS report is: {}".format(date))
            continue
        except arrow.parser.ParserError as e:
            # Most lines will throw value error, don't even log
            pass
        if line.startswith("Station") and "Snow" in line:
            snow_idx = line.index("Snow")
            temp_idx = line.index("Temperature")
            continue
        if line.startswith("Mount Mansfield"):
            try:
                depth_str = line[snow_idx:snow_idx + 4]
                snow_depth = int(depth_str)
                print("Snow depth: {}".format(snow_depth))
            except ValueError:
                print("Error trying to convert string: %s to int").format(depth_str)

            try:
                temp_str = line[temp_idx:temp_idx + 11]
                max_temp, min_temp, cur_temp = [int(i)
                    for i in temp_str.split(' ')
                    if i]  # `if i` gets rid of empty strings
                print("Max temp: {}".format(max_temp))
                print("Min temp: {}".format(min_temp))
                print("Cur temp: {}".format(cur_temp))
            except ValueError:
                print("Error trying to convert {} into temps".format(temp_str))

    # exit if date is none
    if not date:
        print.format("Unable to parse date from NWS report")
        return "No date found"

    update_snow_depth(snow_depth, date)

    return {"Result": "Great success!"}


# @app.route('/wind')
# def wind():
@app.schedule(Rate(1, unit=Rate.HOURS))
def wind(event):
    url = "https://www.weather.gov/btv/mansfield"
    r = requests.get(url)
    html = r.text
    parsed_html = BeautifulSoup(html, "html.parser")
    tbody = parsed_html.body.find("tbody")

    rows = tbody.find_all("tr")[1:]  # first element is table head
    data = []
    for row in rows:
        [timestamp, temperature, direction, wind, gust] = [el.get_text() for el in row.find_all("td")]
        timestamp_format = "YYYY-MM-DD HH:mm:ss"
        # greater than or equal too b/c whitespace
        if len(timestamp) >= len(timestamp_format):
            data.append({
                "timestamp": arrow.get(timestamp, timestamp_format).datetime,
                "temperature": int(temperature),
                "direction": int(direction),
                "wind": int(direction),
                "gust": int(gust)
            })

    data.reverse()
    update_observation(data)
    return {"Result": "Great success!"}


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
    except Exception as e:
        print("move along")


def csv_string_to_list(csv_as_string):
    return list(csv.reader(csv_as_string.split("\n"), delimiter=","))
