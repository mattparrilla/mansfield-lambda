from MesoPy import Meso
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
SYNOPTIC_API_TOKEN = os.getenv('SYNOPTIC_API_TOKEN')
AWS_PROFILE = os.getenv('AWS_PROFILE', 'personal')

# Create boto3 session with profile (for local testing)
# In Lambda, this will use the default session
try:
    session = boto3.Session(profile_name=AWS_PROFILE)
    dynamodb = session.resource('dynamodb')
    s3 = session.client('s3')
    sns = session.client('sns')
except Exception:
    # Fallback to default session (for Lambda environment)
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


def ms_to_kmh(ms):
    """Convert m/s to km/h"""
    if ms is None:
        return None
    return float(ms) * 3.6


def fetch_observations(station: str, hours: int = 2):
    """Fetch recent observations from Synoptic API

    Args:
        station: Station ID (e.g., 'MMNV1')
        hours: Number of hours of historical data to fetch (default 2 hours)

    Returns:
        List of observation dictionaries
    """
    if not SYNOPTIC_API_TOKEN:
        error_msg = "SYNOPTIC_API_TOKEN environment variable not set"
        logger.error(error_msg)
        send_error_notification(error_msg)
        raise ValueError(error_msg)

    try:
        m = Meso(token=SYNOPTIC_API_TOKEN)

        # Calculate time window (last N hours)
        end_time = datetime.utcnow()
        start_time = end_time - timedelta(hours=hours)

        # Format times for API (YYYYMMDDHHmm)
        start_str = start_time.strftime('%Y%m%d%H%M')
        end_str = end_time.strftime('%Y%m%d%H%M')

        # Fetch time series data
        response = m.timeseries(
            stid=station,
            start=start_str,
            end=end_str,
            vars='air_temp,wind_speed,wind_gust,wind_direction,precip_accum'
        )

        if not response or 'STATION' not in response:
            logger.warning("No station data found in API response")
            return []

        station_data = response['STATION'][0]
        observations_data = station_data.get('OBSERVATIONS', {})

        # Extract timestamps and convert to ISO format
        date_times = observations_data.get('date_time', [])
        if not date_times:
            logger.warning("No observations found in API response")
            return []

        observations = []

        # Get all the data arrays
        temps = observations_data.get('air_temp_set_1', [])
        wind_speeds = observations_data.get('wind_speed_set_1', [])
        wind_gusts = observations_data.get('wind_gust_set_1', [])
        wind_dirs = observations_data.get('wind_direction_set_1', [])
        precips = observations_data.get('precip_accum_set_1', [])

        # Process each timestamp
        for i, dt_str in enumerate(date_times):
            # Convert Synoptic datetime to ISO format
            # Synoptic format: "2025-01-13T12:00:00Z"
            timestamp = dt_str

            # Get values for this timestamp (with bounds checking)
            temp_c = temps[i] if i < len(temps) and temps[i] is not None else None
            wind_speed_ms = wind_speeds[i] if i < len(wind_speeds) and wind_speeds[i] is not None else None
            wind_gust_ms = wind_gusts[i] if i < len(wind_gusts) and wind_gusts[i] is not None else None
            wind_dir = wind_dirs[i] if i < len(wind_dirs) and wind_dirs[i] is not None else None
            precip_mm = precips[i] if i < len(precips) and precips[i] is not None else None

            # Convert wind speeds from m/s to km/h
            wind_speed_kmh = ms_to_kmh(wind_speed_ms)
            wind_gust_kmh = ms_to_kmh(wind_gust_ms)

            # Build observation dict (matching nws_observations.py schema)
            observation = {
                'station': station,
                'timestamp': timestamp,
                'temp_c': convert_to_decimal(temp_c),
                'wind_speed_kmh': convert_to_decimal(wind_speed_kmh),
                'wind_gust_kmh': convert_to_decimal(wind_gust_kmh),
                'wind_direction_deg': convert_to_decimal(wind_dir),
                'precip_1h_mm': convert_to_decimal(precip_mm) if precip_mm else None
            }

            # Filter out None values to save space in DynamoDB
            observation = {k: v for k, v in observation.items() if v is not None}

            # Only add if we have a timestamp
            if observation.get('timestamp'):
                observations.append(observation)

        logger.info(f"Fetched {len(observations)} observations from Synoptic API")
        return observations

    except Exception as e:
        error_msg = f"Failed to fetch observations from Synoptic API: {str(e)}"
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


def fetch_nws_reference_data(station: str):
    """Fetch NWS data to use as ground truth for validation

    Args:
        station: Station ID (e.g., 'MMNV1')

    Returns:
        Dict mapping ISO timestamp to temperature in Fahrenheit
    """
    try:
        import requests
        url = f"https://api.weather.gov/stations/{station}/observations?limit=500"
        headers = {"User-Agent": "me@matthewparrilla.com"}
        r = requests.get(url, headers=headers, timeout=10)
        r.raise_for_status()
        data = r.json()

        nws_temps = {}
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            timestamp = props.get('timestamp')
            temp_c = props.get('temperature', {}).get('value')
            if timestamp and temp_c is not None:
                temp_f = (temp_c * 9/5) + 32
                nws_temps[timestamp] = temp_f

        logger.info(f"Fetched {len(nws_temps)} NWS reference observations")
        return nws_temps
    except Exception as e:
        logger.warning(f"Failed to fetch NWS reference data: {str(e)}")
        return {}


def smooth_temperature_data(observations):
    """Validate and correct temperature data using NWS as ground truth

    Cross-references Synoptic data with NWS observations. Where NWS data exists,
    uses it directly. For Synoptic-only data, validates against NWS trends and
    interpolates corrections.

    Args:
        observations: List of observation dicts (sorted by timestamp descending)

    Returns:
        List of observations with corrected temperature data
    """
    from datetime import datetime

    # Fetch NWS reference data
    nws_temps = fetch_nws_reference_data(STATION)

    if not nws_temps:
        logger.warning("No NWS reference data available, using basic range filter only")
        # Fallback to simple range filter
        for obs in observations:
            temp_c = obs.get('temp_c')
            if temp_c:
                temp_f = celsius_to_fahrenheit(temp_c)
                if temp_f < -50 or temp_f > 100:
                    obs.pop('temp_c', None)
        return observations

    # Convert NWS timestamps to datetime objects for interpolation
    nws_times = []
    nws_values = []
    for ts, temp in sorted(nws_temps.items()):
        try:
            dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
            nws_times.append(dt)
            nws_values.append(temp)
        except:
            continue

    if len(nws_values) < 2:
        logger.warning("Insufficient NWS data for validation")
        return observations

    # Get NWS temperature range for bounds checking
    nws_min = min(nws_values)
    nws_max = max(nws_values)
    tolerance = 10  # Allow ±10°F from NWS range

    corrected_observations = []
    for obs in observations:
        obs_copy = obs.copy()
        timestamp = obs['timestamp']
        temp_c = obs.get('temp_c')

        # Check if we have exact NWS match
        if timestamp in nws_temps:
            # Use NWS data directly (most reliable)
            nws_temp_f = nws_temps[timestamp]
            obs_copy['temp_c'] = Decimal(str((nws_temp_f - 32) * 5/9))
        elif temp_c is not None:
            # Validate Synoptic data against NWS range
            temp_f = celsius_to_fahrenheit(temp_c)

            # Check if wildly out of range
            if temp_f < (nws_min - tolerance) or temp_f > (nws_max + tolerance):
                # Bad value - interpolate from NWS data
                try:
                    obs_dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    # Find nearest NWS observations
                    if obs_dt <= nws_times[0]:
                        interp_temp = nws_values[0]
                    elif obs_dt >= nws_times[-1]:
                        interp_temp = nws_values[-1]
                    else:
                        # Linear interpolation
                        for i in range(len(nws_times) - 1):
                            if nws_times[i] <= obs_dt <= nws_times[i + 1]:
                                t0, t1 = nws_times[i], nws_times[i + 1]
                                v0, v1 = nws_values[i], nws_values[i + 1]
                                weight = (obs_dt - t0).total_seconds() / (t1 - t0).total_seconds()
                                interp_temp = v0 + weight * (v1 - v0)
                                break
                        else:
                            interp_temp = nws_values[0]

                    obs_copy['temp_c'] = Decimal(str((interp_temp - 32) * 5/9))
                    logger.debug(f"Corrected outlier at {timestamp}: {temp_f:.1f}°F -> {interp_temp:.1f}°F")
                except:
                    # If interpolation fails, remove the bad value
                    obs_copy.pop('temp_c', None)
            # else: temp is within acceptable range, keep it

        corrected_observations.append(obs_copy)

    return corrected_observations


def write_observations_json(observations, last_above_freezing):
    """Write observations JSON to S3 with imperial units (F, mph)

    Args:
        observations: List of observation dictionaries (in metric)
        last_above_freezing: ISO timestamp string or None
    """
    try:
        # Apply smoothing to remove temperature outliers
        smoothed_observations = smooth_temperature_data(observations)

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
                for obs in smoothed_observations
            ]
        }

        # Convert to JSON string
        json_string = json.dumps(json_data, indent=2)

        # Gzip the JSON
        compressed_json = gzip.compress(json_string.encode('utf-8'))

        # Check if running locally (AWS_PROFILE is set) or in Lambda
        if AWS_PROFILE and os.getenv('AWS_PROFILE'):
            # Running locally - save to file
            local_filename = OBSERVATIONS_JSON + '.gz'
            with open(local_filename, 'wb') as f:
                f.write(compressed_json)
            logger.info(f"Successfully wrote gzipped {local_filename} locally with {len(observations)} observations")
            print(f"Saved {local_filename} locally ({len(compressed_json)} bytes)")
            return True
        else:
            # Running in Lambda - upload to S3
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
        error_msg = f"Failed to write observations JSON: {str(e)}"
        logger.error(error_msg)
        send_error_notification(error_msg)
        return False


def lambda_handler(event=None, context=None):
    """Lambda handler function

    Fetches recent observations from Synoptic API and stores them in DynamoDB.
    Safe to run at any frequency - duplicate timestamps will be overwritten (idempotent).
    """
    try:
        # Fetch 2 hours of observations to ensure we don't miss any
        logger.info(f"Fetching observations for station {STATION}")
        observations = fetch_observations(STATION, hours=2)

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
    print("Testing Synoptic observations fetcher...")
    result = lambda_handler()
    print(f"Result: {result}")
