"""
AGF Data Ingestion Lambda Function

Triggered by S3 EventBridge when run.json or experiment.json files are uploaded.
Processes metadata and populates DynamoDB tables for dashboard queries.

Author: Felix Meier
Version: 1.1
"""

import json
import boto3
import os
from datetime import datetime, timezone
from urllib.parse import unquote_plus
from typing import Dict, List, Any
from decimal import Decimal
from botocore.exceptions import ClientError

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Get table names from environment variables
FILE_INVENTORY_TABLE = os.environ['FILE_INVENTORY_TABLE']
EXPERIMENTS_TABLE = os.environ['EXPERIMENTS_TABLE']
SYNC_RUNS_TABLE = os.environ['SYNC_RUNS_TABLE']

# Configuration for timestamp parsing strictness (default: lenient for quirky instruments)
STRICT_TIMESTAMP_PARSING = os.environ.get('STRICT_TIMESTAMP_PARSING', 'false').lower() == 'true'

# Regex for SHA256 checksum validation
import re
SHA256_PATTERN = re.compile(r'^[a-fA-F0-9]{64}$')

# Initialize table references
file_inventory_table = dynamodb.Table(FILE_INVENTORY_TABLE)
experiments_table = dynamodb.Table(EXPERIMENTS_TABLE)
sync_runs_table = dynamodb.Table(SYNC_RUNS_TABLE)


def parse_timestamp(timestamp_str: str, field_name: str = "timestamp") -> float:
    """
    Parse ISO 8601 timestamp string to Unix timestamp.
    Behavior controlled by STRICT_TIMESTAMP_PARSING env var.

    Args:
        timestamp_str: ISO 8601 formatted timestamp string
        field_name: Name of the field for logging purposes

    Returns:
        Unix timestamp as float
    """
    if not timestamp_str:
        if STRICT_TIMESTAMP_PARSING:
            raise ValueError(f"{field_name} is empty or None")
        print(f"WARNING: {field_name} is empty, using current time")
        return datetime.now(timezone.utc).timestamp()

    try:
        # Handle 'Z' suffix for UTC
        normalized = timestamp_str.replace('Z', '+00:00')
        return datetime.fromisoformat(normalized).timestamp()
    except (ValueError, AttributeError, TypeError) as e:
        if STRICT_TIMESTAMP_PARSING:
            raise ValueError(f"Failed to parse {field_name} '{timestamp_str}': {e}")
        print(f"WARNING: Failed to parse {field_name} '{timestamp_str}': {e}. Using current time.")
        return datetime.now(timezone.utc).timestamp()


def validate_s3_path(key: str) -> bool:
    """
    Basic validation that S3 key has expected structure.
    Lenient to handle variations from different instruments.

    Expected format: raw/{instrument}/{YYYY}/{MM}/{DD}/{run_id}/...
    """
    if not key.startswith('raw/'):
        raise ValueError(f"S3 key must start with 'raw/': {key}")

    parts = key.split('/')
    if len(parts) < 6:
        raise ValueError(f"S3 key too short, expected at least 6 segments: {key}")

    # Check for date-like segments (YYYY/MM/DD)
    try:
        year, month, day = parts[2], parts[3], parts[4]
        if not (year.isdigit() and month.isdigit() and day.isdigit()):
            raise ValueError(f"Expected date segments (YYYY/MM/DD) at positions 2-4: {key}")
    except IndexError:
        raise ValueError(f"Missing date segments in path: {key}")

    return True


def validate_checksum(checksum: str) -> str:
    """
    Validate and normalize SHA256 checksum.

    Args:
        checksum: SHA256 checksum, optionally prefixed with 'sha256:'

    Returns:
        Normalized lowercase checksum (64 hex characters)
    """
    # Remove 'sha256:' prefix if present
    clean = checksum.replace('sha256:', '').strip()

    if not SHA256_PATTERN.match(clean):
        raise ValueError(f"Invalid SHA256 checksum format: {checksum}")

    return clean.lower()


def lambda_handler(event, context):
    """
    Main handler function triggered by S3 events via EventBridge
    """
    print(f"Received event: {json.dumps(event)}")

    processed_count = 0
    error_count = 0

    # Handle EventBridge event format
    if 'detail' in event:
        # EventBridge format: single event
        records = [{
            's3': {
                'bucket': {'name': event['detail']['bucket']['name']},
                'object': {'key': event['detail']['object']['key']}
            }
        }]
    elif 'Records' in event:
        # Direct S3 notification format (for testing/manual invocation)
        records = event['Records']
    else:
        print(f"Unknown event format: {event}")
        return {'statusCode': 400, 'body': 'Unknown event format'}

    for record in records:
        try:
            # Extract S3 event details
            bucket = record['s3']['bucket']['name']
            key = unquote_plus(record['s3']['object']['key'])

            print(f"Processing: s3://{bucket}/{key}")

            # Validate S3 path structure
            try:
                validate_s3_path(key)
            except ValueError as e:
                print(f"WARNING: Skipping invalid path: {e}")
                continue

            # Route to appropriate handler based on file type
            if key.endswith('run.json'):
                process_run_metadata(bucket, key)
                processed_count += 1
            elif key.endswith('experiment.json'):
                process_experiment_metadata(bucket, key)
                processed_count += 1
            else:
                print(f"Skipping non-metadata file: {key}")

        except Exception as e:
            print(f"Error processing record: {str(e)}")
            error_count += 1
            # Don't raise - continue processing other files

    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': processed_count,
            'errors': error_count
        })
    }


def process_run_metadata(bucket: str, key: str):
    """
    Process run.json metadata file

    S3 Key Format: raw/{instrument}/{YYYY}/{MM}/{DD}/{run_id}/run.json
    """
    print(f"Processing run metadata: {key}")

    # Parse S3 path
    path_parts = key.split('/')
    instrument_id = path_parts[1]
    year, month, day = path_parts[2:5]
    run_id = path_parts[5]

    # Download run.json
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        run_data = json.loads(response['Body'].read())
    except Exception as e:
        print(f"Error downloading run.json: {e}")
        raise

    # Convert to Unix timestamp with error handling
    sync_timestamp = parse_timestamp(run_data.get('sync_timestamp', ''), 'sync_timestamp')

    # Calculate total_bytes from file manifest if not provided
    total_bytes = run_data.get('total_size_bytes')
    if total_bytes is None:
        total_bytes = sum(f.get('size', 0) for f in run_data.get('file_manifest', []))

    # Store run metadata in DynamoDB
    item = {
        'run_id': run_id,
        'instrument_id': instrument_id,
        'computer_name': run_data['computer_name'],
        'sync_timestamp': Decimal(str(int(sync_timestamp))),
        'date': f"{year}-{month}-{day}",
        'files_count': run_data['files_in_batch'],
        'total_bytes': total_bytes,
        'staff_names': list(run_data.get('files_by_staff', {}).keys()),
        's3_key': key,
        's3_bucket': bucket,
        'processing_status': 'completed',
        'processed_at': Decimal(str(int(datetime.now(timezone.utc).timestamp())))
    }

    try:
        sync_runs_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(run_id) AND attribute_not_exists(instrument_id)'
        )
        print(f"Stored run metadata: {run_id}")
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        print(f"Run {run_id} already exists, skipping (idempotent)")
    except ClientError as e:
        print(f"ERROR: DynamoDB write failed for run {run_id}: {e}")
        raise

    # Process individual files in the manifest using batch_writer for efficiency
    file_manifest = run_data.get('file_manifest', [])
    if file_manifest:
        file_items = []
        for file_entry in file_manifest:
            file_item = build_file_record(bucket, key, run_id, instrument_id, file_entry)
            if file_item:
                file_items.append(file_item)

        # Use batch_writer for efficient bulk writes
        with file_inventory_table.batch_writer() as batch:
            for item in file_items:
                batch.put_item(Item=item)

        print(f"Batch wrote {len(file_items)} files from run {run_id}")
    else:
        print(f"No files in manifest for run {run_id}")


def process_experiment_metadata(bucket: str, key: str):
    """
    Process experiment.json metadata file

    S3 Key Format: raw/{instrument}/{YYYY}/{MM}/{DD}/{run_id}/{staff}/payload/{experiment}/experiment.json
    """
    print(f"Processing experiment metadata: {key}")

    # Download experiment.json
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        exp_data = json.loads(response['Body'].read())
    except Exception as e:
        print(f"Error downloading experiment.json: {e}")
        raise

    # Convert timestamps to Unix with error handling
    created_at = parse_timestamp(exp_data.get('created', ''), 'created')
    last_updated = parse_timestamp(exp_data.get('last_updated', ''), 'last_updated')

    # Store experiment metadata
    item = {
        'experiment_id': exp_data['experiment_id'],
        'last_updated': Decimal(str(int(last_updated))),
        'experiment_folder': exp_data['experiment_folder'],
        'staff_name': exp_data['staff_name'],
        'instrument_id': exp_data['instrument'],
        'computer_name': exp_data['computer'],
        'created_at': Decimal(str(int(created_at))),
        'update_count': exp_data.get('update_count', 1),
        'file_count': exp_data['file_count'],
        'total_bytes': exp_data['total_size_bytes'],
        's3_location': exp_data['s3_location'],
        's3_experiment_json_key': key,
        's3_bucket': bucket,
        'auto_detected': exp_data.get('auto_detected', True),
        'sync_version': exp_data.get('sync_version', '1.0')
    }

    # Add parameters if present (future ML use)
    if 'parameters' in exp_data:
        item['parameters'] = exp_data['parameters']

    try:
        experiments_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(experiment_id) AND attribute_not_exists(last_updated)'
        )
        print(f"Stored experiment metadata: {exp_data['experiment_id']}")
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        print(f"Experiment {exp_data['experiment_id']} already exists, skipping (idempotent)")
    except ClientError as e:
        print(f"ERROR: DynamoDB write failed for experiment {exp_data['experiment_id']}: {e}")
        raise

    # Store individual file records using batch_writer
    experiment_id = exp_data['experiment_id']
    files = exp_data.get('files', [])

    if files:
        file_items = []
        for file_entry in files:
            file_item = build_experiment_file_record(
                bucket, key, experiment_id,
                exp_data['staff_name'],
                exp_data['instrument'],
                file_entry
            )
            if file_item:
                file_items.append(file_item)

        # Use batch_writer with conditional writes for experiment files
        # Note: batch_writer doesn't support ConditionExpression, so we write individually
        # but still benefit from automatic batching and retry logic
        written_count = 0
        skipped_count = 0
        for file_item in file_items:
            try:
                file_inventory_table.put_item(
                    Item=file_item,
                    ConditionExpression='attribute_not_exists(experiment_id) AND attribute_not_exists(file_path)'
                )
                written_count += 1
            except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
                skipped_count += 1

        print(f"Wrote {written_count} files, skipped {skipped_count} existing files from experiment {experiment_id}")
    else:
        print(f"No files in experiment {experiment_id}")


def build_file_record(bucket: str, run_json_key: str, run_id: str,
                      instrument_id: str, file_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a file record item from run.json manifest entry.

    Note: run.json file_manifest paths are in format: {staff}/{experiment}/file.ext
    But actual S3 keys have 'payload' inserted: {staff}/payload/{experiment}/file.ext

    Returns:
        Dict containing the DynamoDB item, or None if build fails
    """
    # Extract experiment_id from file path
    # run.json format: {staff}/{experiment}/file.ext (NO payload in manifest)
    file_path = file_entry['path']
    path_parts = file_path.split('/')

    staff_name = file_entry.get('staff_name', path_parts[0] if len(path_parts) > 0 else 'unknown')

    # Try to extract experiment folder (position 1 in run.json path, after staff)
    experiment_folder = None
    if len(path_parts) >= 2:
        # path_parts[0] = staff_name, path_parts[1] = experiment folder
        experiment_folder = path_parts[1]

    # Build experiment_id
    if experiment_folder and experiment_folder != path_parts[-1]:
        # Only use as experiment if it's a folder, not the file itself
        experiment_id = f"{experiment_folder}_{staff_name}"
    else:
        experiment_id = f"standalone_{staff_name}_{run_id}"

    # Construct full S3 key
    # The sync script adds 'payload/' between staff name and the rest of the path
    # run.json path: Felix_Meier/TestExp2/file.txt
    # S3 key:        .../run_id/Felix_Meier/payload/TestExp2/file.txt
    run_base = '/'.join(run_json_key.split('/')[:-1])  # Remove run.json

    # Insert 'payload' after staff name in the path
    if len(path_parts) > 1:
        # Has subfolders: staff/experiment/file -> staff/payload/experiment/file
        s3_path = f"{path_parts[0]}/payload/{'/'.join(path_parts[1:])}"
    else:
        # File directly in staff folder: staff/file -> staff/payload/file
        s3_path = f"{path_parts[0]}/payload/{file_path.split('/')[-1]}"

    s3_key = f"{run_base}/{s3_path}"

    # Parse file modification date with error handling
    file_date = parse_timestamp(file_entry.get('file_date', ''), 'file_date')

    # Get file extension
    file_name = file_path.split('/')[-1]
    file_type = file_name.split('.')[-1].lower() if '.' in file_name else 'unknown'

    return {
        'experiment_id': experiment_id,
        'file_path': file_entry['path'],
        'file_name': file_name,
        's3_key': s3_key,
        's3_bucket': bucket,
        'file_size_bytes': file_entry['size'],
        'file_type': file_type,
        'checksum_sha256': validate_checksum(file_entry['checksum']),
        'uploaded_at': Decimal(str(int(datetime.now(timezone.utc).timestamp()))),
        'modified_at': Decimal(str(int(file_date))),
        'run_id': run_id,
        'staff_name': staff_name,
        'instrument_id': instrument_id,
        'is_update': file_entry.get('is_update', False)
    }


def build_experiment_file_record(bucket: str, exp_json_key: str,
                                  experiment_id: str, staff_name: str,
                                  instrument_id: str, file_entry: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a file record item from experiment.json entry.

    Returns:
        Dict containing the DynamoDB item
    """
    # Construct full S3 key
    exp_base = '/'.join(exp_json_key.split('/')[:-1])  # Remove experiment.json
    s3_key = f"{exp_base}/{file_entry['relative_path']}"

    # Parse file modification date with error handling
    file_modified = parse_timestamp(file_entry.get('modified', ''), 'modified')

    # Get file extension
    file_name = file_entry['name']
    file_type = file_name.split('.')[-1].lower() if '.' in file_name else 'unknown'

    return {
        'experiment_id': experiment_id,
        'file_path': file_entry['relative_path'],
        'file_name': file_name,
        's3_key': s3_key,
        's3_bucket': bucket,
        'file_size_bytes': file_entry['size'],
        'file_type': file_type,
        'checksum_sha256': validate_checksum(file_entry['checksum']),
        'uploaded_at': Decimal(str(int(datetime.now(timezone.utc).timestamp()))),
        'modified_at': Decimal(str(int(file_modified))),
        'staff_name': staff_name,
        'instrument_id': instrument_id,
        'run_id': 'from_experiment_json'  # Will be updated if run.json processes this file
    }


def generate_presigned_url(bucket: str, key: str, expiration: int = 3600) -> str:
    """
    Generate pre-signed S3 URL for file download
    """
    try:
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket, 'Key': key},
            ExpiresIn=expiration
        )
        return url
    except Exception as e:
        print(f"Error generating pre-signed URL: {e}")
        return None
