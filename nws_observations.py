import requests
import boto3
import logging
import os
import json
import gzip
from datetime import datetime, timedelta
from decimal import Decimal
from boto3.dynamodb.conditions import Key

STATION = "MMNV1"
TABLE_NAME = os.getenv('OBSERVATIONS_TABLE_NAME', 'MountMansfieldObservations')
S3_BUCKET = 'matthewparrilla.com'
OBSERVATIONS_JSON = 'mansfield-observations.json'

dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')
sns = boto3.client('sns')
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send_error_notification(error_message):
    """Send error notification to SNS topic"""
    if not SNS_TOPIC_ARN:
        logger.warning("SNS_TOPIC_ARN not set, skipping notification")
        return
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='Mount Mansfield Observations Error',
            Message=error_message
        )
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {str(e)}")


def convert_to_decimal(value):
    """Convert float to Decimal for DynamoDB, handling None values"""
    if value is None:
        return None
    return Decimal(str(value))


def fetch_observations(station: str, limit: int = 20):
    """Fetch recent observations from NWS API

    Args:
        station: Station ID (e.g., 'MMNV1')
        limit: Number of observations to fetch (default 20 covers ~1.5 hours at 5min intervals)

    Returns:
        List of observation dictionaries
    """
    url = f"https://api.weather.gov/stations/{station}/observations?limit={limit}"
    headers = {"User-Agent": "me@matthewparrilla.com"}

    try:
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        if not data.get("features"):
            logger.warning("No observations found in API response")
            return []

        observations = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})

            # Extract the fields we care about
            observation = {
                'station': station,
                'timestamp': props.get('timestamp'),
                'temp_c': convert_to_decimal(props.get('temperature', {}).get('value')),
                'wind_speed_kmh': convert_to_decimal(props.get('windSpeed', {}).get('value')),
                'wind_gust_kmh': convert_to_decimal(props.get('windGust', {}).get('value')),
                'wind_direction_deg': convert_to_decimal(props.get('windDirection', {}).get('value')),
                'wind_chill_c': convert_to_decimal(props.get('windChill', {}).get('value')),
                'text_desc': props.get('textDescription'),
                'precip_1h_mm': convert_to_decimal(props.get('precipitationLastHour', {}).get('value'))
            }

            # Filter out None values to save space in DynamoDB
            observation = {k: v for k, v in observation.items() if v is not None}

            # Only add if we have a timestamp
            if observation.get('timestamp'):
                observations.append(observation)

        return observations

    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to fetch observations from NWS API: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        raise
    except Exception as e:
        error_msg = f"Unexpected error fetching observations: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        raise


def store_observation(observation):
    """Store observation in DynamoDB"""
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.put_item(Item=observation)
        logger.info(f"Stored observation for {observation['timestamp']}")
        return True
    except Exception as e:
        error_msg = f"Failed to store observation in DynamoDB: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        raise


def get_last_n_days_observations(station: str, days: int = 10):
    """Query DynamoDB for observations from the last N days

    Args:
        station: Station ID (e.g., 'MMNV1')
        days: Number of days to query (default 10)

    Returns:
        List of observation dictionaries sorted by timestamp (newest first)
    """
    try:
        table = dynamodb.Table(TABLE_NAME)

        # Calculate cutoff timestamp (N days ago)
        cutoff = datetime.utcnow() - timedelta(days=days)
        cutoff_iso = cutoff.isoformat() + 'Z'

        # Query DynamoDB - station is hash key, timestamp is range key
        response = table.query(
            KeyConditionExpression=Key('station').eq(station) & Key('timestamp').gte(cutoff_iso)
        )

        observations = response.get('Items', [])

        # Sort by timestamp descending (newest first)
        observations.sort(key=lambda x: x['timestamp'], reverse=True)

        logger.info(f"Retrieved {len(observations)} observations from last {days} days")
        return observations

    except Exception as e:
        logger.error(f"Failed to query observations from DynamoDB: {str(e)}")
        return []


def find_last_above_freezing(observations):
    """Find the last datetime when temperature was above freezing (32°F / 0°C)

    If the most recent non-null temperature is above freezing, returns the
    timestamp of the most recent observation (assuming we're still above freezing).
    Otherwise, returns the last timestamp when temperature was above freezing.

    Args:
        observations: List of observation dicts sorted by timestamp descending

    Returns:
        ISO timestamp string of last above-freezing observation, or None
    """
    if not observations:
        return None

    # Find the most recent non-null temperature
    most_recent_temp = None

    for obs in observations:
        temp_c = obs.get('temp_c')
        if temp_c is not None:
            most_recent_temp = float(temp_c)
            break

    # If no temperature data at all
    if most_recent_temp is None:
        return None

    # If most recent temperature is above freezing,
    # assume we're still above freezing now (return most recent timestamp)
    if most_recent_temp > 0:
        return observations[0]['timestamp']

    # Otherwise, most recent temp is below/at freezing,
    # so find the last time we were above freezing
    for obs in observations:
        temp_c = obs.get('temp_c')
        if temp_c and float(temp_c) > 0:
            return obs['timestamp']

    return None


def decimal_to_float(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def celsius_to_fahrenheit(celsius):
    """Convert Celsius to Fahrenheit"""
    if celsius is None:
        return None
    return (float(celsius) * 9/5) + 32


def kmh_to_mph(kmh):
    """Convert km/h to mph"""
    if kmh is None:
        return None
    return float(kmh) * 0.621371


def mm_to_inches(mm):
    """Convert mm to inches"""
    if mm is None:
        return None
    return float(mm) * 0.0393701


def write_observations_json(observations, last_above_freezing):
    """Write observations JSON to S3 with imperial units (F, mph)

    Args:
        observations: List of observation dictionaries (in metric)
        last_above_freezing: ISO timestamp string or None
    """
    try:
        # Prepare JSON structure with imperial units
        json_data = {
            'last_updated': datetime.utcnow().isoformat() + 'Z',
            'last_above_freezing': last_above_freezing,
            'observations': [
                {
                    'timestamp': obs['timestamp'],
                    'temperature_f': round(celsius_to_fahrenheit(obs.get('temp_c')), 1) if obs.get('temp_c') else None,
                    'wind_speed_mph': round(kmh_to_mph(obs.get('wind_speed_kmh')), 1) if obs.get('wind_speed_kmh') else None,
                    'wind_direction_deg': float(obs['wind_direction_deg']) if obs.get('wind_direction_deg') else None,
                    'precip_1h_in': round(mm_to_inches(obs.get('precip_1h_mm')), 2) if obs.get('precip_1h_mm') else None
                }
                for obs in observations
            ]
        }

        # Convert to JSON string
        json_string = json.dumps(json_data, indent=2)

        # Gzip the JSON
        compressed_json = gzip.compress(json_string.encode('utf-8'))

        # Upload to S3
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=OBSERVATIONS_JSON,
            Body=compressed_json,
            ContentType='application/json',
            ContentEncoding='gzip',
            ACL='public-read'
        )

        logger.info(f"Successfully wrote gzipped {OBSERVATIONS_JSON} to S3 with {len(observations)} observations")
        return True

    except Exception as e:
        error_msg = f"Failed to write observations JSON to S3: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        return False


def lambda_handler(event=None, context=None):
    """Lambda handler function

    Fetches multiple recent observations and stores them in DynamoDB.
    Safe to run at any frequency - duplicate timestamps will be overwritten (idempotent).
    """
    try:
        # Fetch 20 observations (covers ~1.5 hours at 5min intervals)
        # Adjust this based on your schedule:
        # - Every 5 min: limit=20 is plenty
        # - Hourly: limit=15 covers the hour
        # - Daily: limit=300 covers the full day (24h * 12 obs/hour)
        logger.info(f"Fetching observations for station {STATION}")
        observations = fetch_observations(STATION, limit=20)

        if not observations:
            logger.warning("No observations retrieved from API")
            return {
                'statusCode': 200,
                'body': 'No observation data available'
            }

        # Store each observation (duplicates will be overwritten)
        stored_count = 0
        for obs in observations:
            store_observation(obs)
            stored_count += 1

        logger.info(f"Successfully stored {stored_count} observations")

        # Write JSON file with last 10 days of observations
        logger.info("Fetching last 10 days of observations for JSON export")
        last_10_days = get_last_n_days_observations(STATION, days=10)

        if last_10_days:
            last_above_freezing = find_last_above_freezing(last_10_days)
            write_observations_json(last_10_days, last_above_freezing)
        else:
            logger.warning("No observations found for JSON export")

        return {
            'statusCode': 200,
            'body': f'Successfully stored {stored_count} observations and wrote JSON'
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }


if __name__ == "__main__":
    # For local testing
    print("Testing NWS observations fetcher...")
    result = lambda_handler()
    print(f"Result: {result}")
