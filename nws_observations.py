import requests
import boto3
import logging
import os
from datetime import datetime
from decimal import Decimal

STATION = "MMNV1"
TABLE_NAME = os.getenv('OBSERVATIONS_TABLE_NAME', 'MountMansfieldObservations')

dynamodb = boto3.resource('dynamodb')
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
                'precip_3h_mm': convert_to_decimal(props.get('precipitationLast3Hours', {}).get('value'))
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
        return {
            'statusCode': 200,
            'body': f'Successfully stored {stored_count} observations'
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
