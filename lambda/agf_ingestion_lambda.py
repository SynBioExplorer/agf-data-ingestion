"""
AGF Data Ingestion Lambda Function

Triggered by S3 EventBridge when run.json or experiment.json files are uploaded.
Processes metadata and populates DynamoDB tables for dashboard queries.

Author: Felix Meier
Version: 1.0
"""

import json
import boto3
import os
from datetime import datetime
from urllib.parse import unquote_plus
from typing import Dict, List, Any
from decimal import Decimal

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# Get table names from environment variables
FILE_INVENTORY_TABLE = os.environ['FILE_INVENTORY_TABLE']
EXPERIMENTS_TABLE = os.environ['EXPERIMENTS_TABLE']
SYNC_RUNS_TABLE = os.environ['SYNC_RUNS_TABLE']

# Initialize table references
file_inventory_table = dynamodb.Table(FILE_INVENTORY_TABLE)
experiments_table = dynamodb.Table(EXPERIMENTS_TABLE)
sync_runs_table = dynamodb.Table(SYNC_RUNS_TABLE)


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
    
    # Convert to Unix timestamp
    sync_timestamp = datetime.fromisoformat(
        run_data['sync_timestamp'].replace('Z', '+00:00')
    ).timestamp()

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
        'processed_at': Decimal(str(int(datetime.now().timestamp())))
    }
    
    sync_runs_table.put_item(Item=item)
    print(f"Stored run metadata: {run_id}")
    
    # Process individual files in the manifest
    for file_entry in run_data.get('file_manifest', []):
        store_file_record(bucket, key, run_id, instrument_id, file_entry)
    
    print(f"Processed {len(run_data.get('file_manifest', []))} files from run {run_id}")


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
    
    # Convert timestamps to Unix
    created_at = datetime.fromisoformat(
        exp_data['created'].replace('Z', '+00:00')
    ).timestamp()
    last_updated = datetime.fromisoformat(
        exp_data['last_updated'].replace('Z', '+00:00')
    ).timestamp()
    
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
    
    experiments_table.put_item(Item=item)
    print(f"Stored experiment metadata: {exp_data['experiment_id']}")
    
    # Store individual file records
    experiment_id = exp_data['experiment_id']
    for file_entry in exp_data.get('files', []):
        store_experiment_file_record(
            bucket, key, experiment_id, 
            exp_data['staff_name'], 
            exp_data['instrument'],
            file_entry
        )
    
    print(f"Processed {len(exp_data.get('files', []))} files from experiment {experiment_id}")


def store_file_record(bucket: str, run_json_key: str, run_id: str,
                     instrument_id: str, file_entry: Dict[str, Any]):
    """
    Store individual file record from run.json manifest

    Note: run.json file_manifest paths are in format: {staff}/{experiment}/file.ext
    But actual S3 keys have 'payload' inserted: {staff}/payload/{experiment}/file.ext
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
    
    # Parse file modification date
    file_date = datetime.fromisoformat(
        file_entry['file_date'].replace('Z', '+00:00')
    ).timestamp()
    
    # Get file extension
    file_name = file_path.split('/')[-1]
    file_type = file_name.split('.')[-1].lower() if '.' in file_name else 'unknown'
    
    item = {
        'experiment_id': experiment_id,
        'file_path': file_entry['path'],
        'file_name': file_name,
        's3_key': s3_key,
        's3_bucket': bucket,
        'file_size_bytes': file_entry['size'],
        'file_type': file_type,
        'checksum_sha256': file_entry['checksum'].replace('sha256:', ''),
        'uploaded_at': Decimal(str(int(datetime.now().timestamp()))),
        'modified_at': Decimal(str(int(file_date))),
        'run_id': run_id,
        'staff_name': staff_name,
        'instrument_id': instrument_id,
        'is_update': file_entry.get('is_update', False)
    }
    
    file_inventory_table.put_item(Item=item)


def store_experiment_file_record(bucket: str, exp_json_key: str, 
                                 experiment_id: str, staff_name: str,
                                 instrument_id: str, file_entry: Dict[str, Any]):
    """
    Store individual file record from experiment.json
    """
    # Construct full S3 key
    exp_base = '/'.join(exp_json_key.split('/')[:-1])  # Remove experiment.json
    s3_key = f"{exp_base}/{file_entry['relative_path']}"
    
    # Parse file modification date
    file_modified = datetime.fromisoformat(
        file_entry['modified'].replace('Z', '+00:00')
    ).timestamp()
    
    # Get file extension
    file_name = file_entry['name']
    file_type = file_name.split('.')[-1].lower() if '.' in file_name else 'unknown'
    
    item = {
        'experiment_id': experiment_id,
        'file_path': file_entry['relative_path'],
        'file_name': file_name,
        's3_key': s3_key,
        's3_bucket': bucket,
        'file_size_bytes': file_entry['size'],
        'file_type': file_type,
        'checksum_sha256': file_entry['checksum'].replace('sha256:', ''),
        'uploaded_at': Decimal(str(int(datetime.now().timestamp()))),
        'modified_at': Decimal(str(int(file_modified))),
        'staff_name': staff_name,
        'instrument_id': instrument_id,
        'run_id': 'from_experiment_json'  # Will be updated if run.json processes this file
    }
    
    # Use conditional write to avoid overwriting data from run.json
    try:
        file_inventory_table.put_item(
            Item=item,
            ConditionExpression='attribute_not_exists(experiment_id) AND attribute_not_exists(file_path)'
        )
    except dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
        print(f"File already exists in inventory (from run.json): {file_entry['name']}")


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
