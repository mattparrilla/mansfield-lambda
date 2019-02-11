from chalice import Chalice, Rate
from bs4 import BeautifulSoup
from datetime import datetime
from botocore.exceptions import ClientError
import requests
import boto3

app = Chalice(app_name='nws_parser')
app.debug = True

SENDER = "BugBucket <support@bugbucket.io>"
SUBJECT = "Mansfield Snow Depth"
CHARSET = "UTF-8"
client = boto3.client("ses")


# @app.route('/')
# def index():

@app.schedule(Rate(1, unit=Rate.HOURS))
def index(event):
    url = "https://forecast.weather.gov/product.php?site=BTV&issuedby=BTV&product=HYD&format=CI&version=1&glossary=0&fbclid=IwAR3IA7pAQ0IG6PAH6m03ahAWPGCSfcq8uObm0wuzvZ19IdOkrGXXbx_y9vc"
    r = requests.get(url)
    html = r.text
    parsed_html = BeautifulSoup(html, 'html.parser')
    pre = parsed_html.body.find('pre', attrs={'class': 'glossaryProduct'})

    date = datetime.today()
    snow_idx = 0
    snow_depth = None
    for line in pre.get_text().split('\n'):
        try:
            # date is of format "1000 AM EST Sun Feb 10 2019"
            date = datetime.strptime(line, "%I%M %p %Z %a %b %d %Y")
            print date
            continue
        except ValueError:
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

    try:
        # Provide the contents of the email.
        client.send_email(
            Destination={
                'ToAddresses': ['matthew.parrilla@gmail.com'],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': """
                            Date: %s
                            Depth: %d
                            """ % (date.strftime("%D"), snow_depth)

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

    return "Great success!"
