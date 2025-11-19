import requests
import boto3
import logging
import os
import csv
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from io import StringIO
from boto3.dynamodb.conditions import Key

# CoCoRaHS stations of interest in Vermont
STATIONS = [
    "VT-WS-41",
    "VT-WS-42",
    "VT-WS-36",
    "VT-WS-19",
    "VT-LM-1",
    "VT-CH-57",
    "VT-LM-23"
]

TABLE_NAME = os.getenv('COCORAHS_TABLE_NAME', 'CocorahsSnowDepth')
AWS_PROFILE = os.getenv('AWS_PROFILE', 'personal')

# Create boto3 session with profile (for local testing)
# In Lambda, this will use the default session
try:
    session = boto3.Session(profile_name=AWS_PROFILE)
    dynamodb = session.resource('dynamodb')
    sns = session.client('sns')
except Exception:
    # Fallback to default session (for Lambda environment)
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
            Subject='CoCoRaHS Observations Error',
            Message=error_message
        )
    except Exception as e:
        logger.error(f"Failed to send SNS notification: {str(e)}")


def convert_to_decimal(value):
    """Convert float to Decimal for DynamoDB, handling None values"""
    if value is None or value == '' or value == 'T' or value == 'NA':
        return None
    try:
        return Decimal(str(value))
    except (ValueError, InvalidOperation):
        return None


def fetch_cocorahs_data(state='VT', days_back=1):
    """Fetch CoCoRaHS daily reports from their export API

    Args:
        state: State code (default 'VT' for Vermont)
        days_back: Number of days to look back (default 1)

    Returns:
        List of observation dictionaries
    """
    # Calculate date range - get yesterday's data since today's may not be complete
    end_date = datetime.utcnow() - timedelta(days=1)
    start_date = end_date - timedelta(days=days_back)

    # Format dates as M/D/YYYY for CoCoRaHS API
    # Use %#m/%#d/%Y on Windows, %-m/%-d/%Y on Unix
    # For cross-platform, manually strip leading zeros
    start_str = f"{start_date.month}/{start_date.day}/{start_date.year}"
    end_str = f"{end_date.month}/{end_date.day}/{end_date.year}"

    # CoCoRaHS export URL
    url = "http://data.cocorahs.org/export/exportreports.aspx"
    params = {
        'ReportType': 'Daily',
        'dtf': '1',
        'Format': 'CSV',
        'State': state,
        'ReportDateType': 'reportdate',
        'StartDate': start_str,
        'EndDate': end_str,
        'TimesInGMT': 'False'
    }

    try:
        logger.info(f"Fetching CoCoRaHS data from {start_str} to {end_str}")
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        # Parse CSV data
        csv_data = StringIO(response.text)
        reader = csv.DictReader(csv_data)

        observations = []
        all_stations = set()  # Track all stations we see
        for row in reader:
            station_number = row.get('StationNumber', '').strip()
            all_stations.add(station_number)

            # Only process stations we're interested in
            if station_number not in STATIONS:
                continue

            # Extract observation date
            observation_date = row.get('ObservationDate', '')
            if not observation_date:
                continue

            # Parse snow depth (TotalSnowDepth)
            snow_depth_in = row.get('TotalSnowDepth', '')

            # Parse other potentially useful fields
            snowfall_in = row.get('TotalSnowfall', '')
            precip_in = row.get('TotalPrecipAmt', '')

            observation = {
                'station': station_number,
                'observation_date': observation_date,  # Format: YYYY-MM-DD
                'snow_depth_in': convert_to_decimal(snow_depth_in),
                'snowfall_in': convert_to_decimal(snowfall_in),
                'precip_in': convert_to_decimal(precip_in),
                'station_name': row.get('StationName', ''),
                'last_updated': datetime.utcnow().isoformat() + 'Z'
            }

            # Filter out None values
            observation = {k: v for k, v in observation.items() if v is not None}

            # Only add if we have a date
            if observation.get('observation_date'):
                observations.append(observation)

        logger.info(f"Fetched {len(observations)} observations from CoCoRaHS for target stations")
        return observations

    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to fetch CoCoRaHS data: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        raise
    except Exception as e:
        error_msg = f"Unexpected error fetching CoCoRaHS data: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        raise


def store_observation(observation):
    """Store observation in DynamoDB"""
    try:
        table = dynamodb.Table(TABLE_NAME)
        table.put_item(Item=observation)
        logger.info(f"Stored observation for {observation['station']} on {observation['observation_date']}")
        return True
    except Exception as e:
        error_msg = f"Failed to store observation in DynamoDB: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        raise


def get_latest_observation(station):
    """Get the most recent observation for a station

    Args:
        station: Station name (e.g., 'VT-WS-41')

    Returns:
        Observation dictionary or None
    """
    try:
        table = dynamodb.Table(TABLE_NAME)

        response = table.query(
            KeyConditionExpression=Key('station').eq(station),
            ScanIndexForward=False,  # Sort descending (newest first)
            Limit=1
        )

        items = response.get('Items', [])
        return items[0] if items else None

    except Exception as e:
        logger.error(f"Failed to query latest observation: {str(e)}")
        return None


def lambda_handler(event=None, context=None):
    """Lambda handler function

    Fetches recent CoCoRaHS observations and stores them in DynamoDB.
    Runs hourly to check for new data.
    """
    try:
        # Fetch last 2 days to ensure we don't miss any late reports
        logger.info("Fetching CoCoRaHS observations for Vermont stations")
        observations = fetch_cocorahs_data(state='VT', days_back=2)

        if not observations:
            logger.warning("No observations retrieved from CoCoRaHS")
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
            'body': f'Successfully stored {stored_count} CoCoRaHS observations'
        }

    except Exception as e:
        logger.error(f"Lambda execution failed: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error: {str(e)}'
        }


if __name__ == "__main__":
    # For local testing
    import sys

    # Enable debug logging to console
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)

    print("Testing CoCoRaHS observations fetcher...")
    print("Note: DynamoDB errors are expected when running locally without AWS credentials\n")

    # Test data fetch
    try:
        observations = fetch_cocorahs_data(state='VT', days_back=7)
        print(f"\nSuccessfully fetched {len(observations)} observations")
        if observations:
            print(f"Sample observation: {observations[0]}")
        else:
            print("No observations found for target stations in the date range")
    except Exception as e:
        print(f"Error fetching data: {e}")
