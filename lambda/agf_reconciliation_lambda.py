"""
AGF S3-DynamoDB Reconciliation Lambda Function

Triggered weekly by CloudWatch Events to compare S3 inventory with DynamoDB records.
Identifies orphaned files (in S3 but not in DynamoDB) and sends email notification
with actionable options.

Author: Felix Meier
Version: 1.0
"""

import json
import boto3
import os
import csv
import gzip
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple
from io import StringIO, BytesIO

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
sns = boto3.client('sns')

# Environment variables
FILE_INVENTORY_TABLE = os.environ.get('FILE_INVENTORY_TABLE', 'agf-file-inventory-dev')
DATA_BUCKET = os.environ.get('DATA_BUCKET', 'agf-instrument-data')
INVENTORY_BUCKET = os.environ.get('INVENTORY_BUCKET', 'agf-instrument-data')
ALERT_EMAIL = os.environ.get('ALERT_EMAIL', 'felix.meier@mq.edu.au')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
BACKFILL_LAMBDA_ARN = os.environ.get('BACKFILL_LAMBDA_ARN', '')

# Initialize table reference
file_inventory_table = dynamodb.Table(FILE_INVENTORY_TABLE)


def lambda_handler(event, context):
    """
    Main handler function triggered by CloudWatch Events (weekly schedule)
    or S3 Inventory completion notification.
    """
    print(f"Starting S3-DynamoDB reconciliation at {datetime.now(timezone.utc).isoformat()}")

    try:
        # Get S3 keys from inventory or direct listing
        s3_keys = get_s3_data_keys()
        print(f"Found {len(s3_keys)} data files in S3")

        # Get DynamoDB s3_key values
        dynamodb_keys = get_dynamodb_s3_keys()
        print(f"Found {len(dynamodb_keys)} file records in DynamoDB")

        # Find discrepancies
        orphaned_in_s3 = s3_keys - dynamodb_keys  # Files in S3 but not in DynamoDB
        orphaned_in_db = dynamodb_keys - s3_keys  # Records in DynamoDB but file missing from S3

        print(f"Orphaned in S3 (no DB record): {len(orphaned_in_s3)}")
        print(f"Orphaned in DB (no S3 file): {len(orphaned_in_db)}")

        # Generate report
        report = generate_report(s3_keys, dynamodb_keys, orphaned_in_s3, orphaned_in_db)

        # Send notification if there are discrepancies
        if orphaned_in_s3 or orphaned_in_db:
            send_notification(report, orphaned_in_s3, orphaned_in_db)
            status = 'discrepancies_found'
        else:
            print("No discrepancies found - S3 and DynamoDB are in sync")
            status = 'in_sync'

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': status,
                's3_count': len(s3_keys),
                'dynamodb_count': len(dynamodb_keys),
                'orphaned_in_s3': len(orphaned_in_s3),
                'orphaned_in_db': len(orphaned_in_db)
            })
        }

    except Exception as e:
        print(f"Error during reconciliation: {str(e)}")
        send_error_notification(str(e))
        raise


def get_s3_data_keys() -> Set[str]:
    """
    Get all data file keys from S3 bucket (excluding .json metadata files).
    Uses pagination to handle large buckets.
    """
    keys = set()
    paginator = s3.get_paginator('list_objects_v2')

    # Only look in raw/ prefix where instrument data is stored
    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix='raw/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Exclude metadata files and folders
            if not key.endswith('/') and not key.endswith('.json'):
                keys.add(key)

    return keys


def get_dynamodb_s3_keys() -> Set[str]:
    """
    Get all s3_key values from DynamoDB file inventory table.
    Uses pagination to handle large tables.
    """
    keys = set()

    # Scan the table to get all s3_key values
    scan_kwargs = {
        'ProjectionExpression': 's3_key',
    }

    done = False
    start_key = None

    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key

        response = file_inventory_table.scan(**scan_kwargs)
        items = response.get('Items', [])

        for item in items:
            if 's3_key' in item:
                keys.add(item['s3_key'])

        start_key = response.get('LastEvaluatedKey')
        done = start_key is None

    return keys


def generate_report(s3_keys: Set[str], dynamodb_keys: Set[str],
                   orphaned_in_s3: Set[str], orphaned_in_db: Set[str]) -> str:
    """
    Generate a human-readable report of the reconciliation results.
    """
    report_lines = [
        "=" * 60,
        "AGF S3-DynamoDB RECONCILIATION REPORT",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "=" * 60,
        "",
        "SUMMARY",
        "-" * 40,
        f"Total files in S3:        {len(s3_keys):,}",
        f"Total records in DynamoDB: {len(dynamodb_keys):,}",
        "",
        f"Orphaned in S3 (no DB record):    {len(orphaned_in_s3):,}",
        f"Orphaned in DB (no S3 file):      {len(orphaned_in_db):,}",
        "",
    ]

    if orphaned_in_s3:
        report_lines.extend([
            "FILES IN S3 WITHOUT DYNAMODB RECORD (sample, max 20):",
            "-" * 40,
        ])
        for key in list(orphaned_in_s3)[:20]:
            report_lines.append(f"  - {key}")
        if len(orphaned_in_s3) > 20:
            report_lines.append(f"  ... and {len(orphaned_in_s3) - 20} more")
        report_lines.append("")

    if orphaned_in_db:
        report_lines.extend([
            "DYNAMODB RECORDS WITHOUT S3 FILE (sample, max 20):",
            "-" * 40,
        ])
        for key in list(orphaned_in_db)[:20]:
            report_lines.append(f"  - {key}")
        if len(orphaned_in_db) > 20:
            report_lines.append(f"  ... and {len(orphaned_in_db) - 20} more")
        report_lines.append("")

    report_lines.extend([
        "RECOMMENDED ACTIONS:",
        "-" * 40,
    ])

    if orphaned_in_s3:
        report_lines.extend([
            "",
            "For files in S3 without DynamoDB records:",
            "  Option 1: Run backfill script to process these files",
            "            python3 scripts/backfill_s3_data.py --bucket agf-instrument-data",
            "  Option 2: Investigate why metadata wasn't created",
            "",
        ])

    if orphaned_in_db:
        report_lines.extend([
            "",
            "For DynamoDB records without S3 files:",
            "  Option 1: Delete orphaned DynamoDB records (if S3 files were intentionally removed)",
            "  Option 2: Investigate - S3 files should be immutable",
            "",
        ])

    report_lines.append("=" * 60)

    return "\n".join(report_lines)


def send_notification(report: str, orphaned_in_s3: Set[str], orphaned_in_db: Set[str]):
    """
    Send email notification with reconciliation report.
    """
    subject = f"[AGF] S3-DynamoDB Reconciliation: {len(orphaned_in_s3) + len(orphaned_in_db)} discrepancies found"

    # Try SNS first, fall back to SES
    if SNS_TOPIC_ARN:
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject[:100],  # SNS subject limit
                Message=report
            )
            print(f"Notification sent via SNS to {SNS_TOPIC_ARN}")
            return
        except Exception as e:
            print(f"Failed to send SNS notification: {e}")

    # Fallback: Try SES
    try:
        ses.send_email(
            Source=ALERT_EMAIL,
            Destination={'ToAddresses': [ALERT_EMAIL]},
            Message={
                'Subject': {'Data': subject},
                'Body': {'Text': {'Data': report}}
            }
        )
        print(f"Notification sent via SES to {ALERT_EMAIL}")
    except Exception as e:
        print(f"Failed to send SES notification: {e}")
        print(f"Report:\n{report}")


def send_error_notification(error_message: str):
    """
    Send notification about reconciliation errors.
    """
    subject = "[AGF] S3-DynamoDB Reconciliation ERROR"
    body = f"""
AGF S3-DynamoDB Reconciliation encountered an error:

{error_message}

Please investigate the Lambda logs for more details.

Time: {datetime.now(timezone.utc).isoformat()}
"""

    if SNS_TOPIC_ARN:
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=body
            )
        except Exception as e:
            print(f"Failed to send error notification: {e}")
