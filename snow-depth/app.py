from chalice import Chalice
import boto3

app = Chalice(app_name='snow-depth')
app.debug = True

s3 = boto3.resource('s3')


@app.route('/')
# @app.schedule('cron(0 12-18/2 * 1,2,3,4,5,9,10,11 ? *)')
def index():
    print "I ran!"
    existing_data = s3.Object(bucket_name='matthewparrilla.com', key='snowDepth.csv')\
        .get()\
        .get('Body')\
        .read()
    print type(existing_data)
    return existing_data
