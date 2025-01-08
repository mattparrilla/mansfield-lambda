from bs4 import BeautifulSoup
import arrow
import requests
import boto3
import csv
import io
import gzip

TEMPERATURE_CSV = "mansfield_temperature.csv"
OBSERVATIONS_CSV = "mansfield_observations.csv"
SNOW_DEPTH_CSV_WRITE = "snowDepth2.csv"  # Update to save to snowDepth2.csv
SNOW_DEPTH_CSV_READ = "snowDepth.csv"  # Read from snowDepth.csv
AVG_SEASON = "Average Season"

s3 = boto3.resource('s3')

def data_to_csv_on_s3(data, filename):
    """Write data to CSV, gzipping and pushing to S3 at filepath"""

    # write list to csv string
    print("Writing data to CSV string")
    new_csv = io.StringIO()
    writer = csv.writer(new_csv, quoting=csv.QUOTE_NONNUMERIC)
    writer.writerows(data)

    print("Compressing data")
    compressed_csv = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_csv, mode="w") as f:
        f.write(new_csv.getvalue().encode('utf-8'))

    print("Pushing to S3")
    s3.Object("matthewparrilla.com", filename).put(
        Body=compressed_csv.getvalue(),
        ContentEncoding='gzip',
        ACL="public-read")

def update_observation(data):
    print("Updating current observation data")

    existing_data = s3.Object(bucket_name='matthewparrilla.com',
        key=OBSERVATIONS_CSV).get().get('Body').read()

    observation_data = gzip.GzipFile(fileobj=io.BytesIO(existing_data)).read().decode('utf-8')

    data_as_list = [i for i in csv_string_to_list(observation_data) if len(i) > 0]
    last_observation = data_as_list[-1]
    timestamp = arrow.get(last_observation[0])

    for observation in data:
        observation_timestamp = observation["timestamp"]
        if arrow.get(observation_timestamp) > timestamp:
            print("Adding observation for: {}".format(observation_timestamp))
            data_as_list.append([
                observation["timestamp"].isoformat(),
                observation["temperature"],
                observation["direction"],
                observation["wind"],
                observation["gust"]])

    data_to_csv_on_s3(data_as_list, OBSERVATIONS_CSV)

def update_snow_depth(snow_depth, date):
    # exit if we don't get a reported depth
    if snow_depth is None:
        print("No depth in current text report")
        return

    # get current data from s3
    existing_data = s3.Object(bucket_name='matthewparrilla.com',
        key=SNOW_DEPTH_CSV_READ)\
        .get()\
        .get('Body')\
        .read()
    uncompressed_file = gzip.GzipFile(fileobj=io.BytesIO(existing_data)).read().decode('utf-8')

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

    # it's our first day of the season, add a row
    if len(data) == season_index:
        data.append([None] * len(data[0]))
        data[-1][0] = season

    # check if we need to update data
    try:
        if data[season_index][date_index] == str(snow_depth):
            print("No new snow depth data. Exiting")
            return False
        else:
            # update copy of data
            data[season_index][date_index] = snow_depth
            print("Fresh data. Time for an update")

    except IndexError:
        print("season_index: {}".format(season_index))
        print("date_index:   {}".format(date_index))
        print("len(data):    {}".format(len(data)))

    data = calculate_average([d for d in data if d[0] != AVG_SEASON])

    data_to_csv_on_s3(data, SNOW_DEPTH_CSV_WRITE)

def csv_string_to_list(csv_as_string):
    return list(csv.reader(csv_as_string.split("\n"), delimiter=","))

def calculate_average(data):
    """Find the average depth of all seasons across all dates. Expects data
       to be a list of seasons where the first item in each
       season is the name of the season and the first row in the data is
       a header row"""
    avg_season = [None] * len(data[0])
    avg_season[0] = AVG_SEASON

    # iterate through each date
    for i, d in enumerate(data[0]):
        if i == 0:  # first item is label
            continue
        depths = 0
        depth_count = 0
        # iterate through seasons
        for j, season in enumerate(data):
            if j == 0:  # first item is header row
                continue
            # If we have no data, don't use in average. No data values are an
            # empty string. In this case, since the data returned is a string
            # '0' won't get filtered out
            elif not season[i]:
                continue
            else:
                depths += int(season[i])
                depth_count += 1

        avg_season[i] = depths / depth_count

    data.append(avg_season)
    return data

def snow_depth(event, context):
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
                print("Error trying to convert string: {} to int".format(depth_str))

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
        print("Unable to parse date from NWS report")
        return {"Result": "No date found"}

    update_snow_depth(snow_depth, date)

    return {"Result": "Great success!"}

def observation(event, context):
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
                "wind": int(wind),
                "gust": int(gust)
            })

    data.reverse()
    update_observation(data)
    return {"Result": "Great success!"}