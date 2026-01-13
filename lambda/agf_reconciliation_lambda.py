"""
AGF S3-DynamoDB Reconciliation Lambda Function

Triggered weekly by CloudWatch Events to compare S3 inventory with DynamoDB records.
Identifies orphaned files (in S3 but not in DynamoDB) and sends email notification
with actionable options.

Checks ALL relevant DynamoDB tables:
- agf-file-inventory: tracks data files (via s3_key)
- agf-sync-runs: tracks run.json files (via s3_key)
- agf-experiments: tracks experiment.json files (via s3_experiment_json_key)

Author: Felix Meier
Version: 1.1
"""

import json
import boto3
import os
from datetime import datetime, timezone
from typing import Set

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
sns = boto3.client('sns')
cloudwatch = boto3.client('cloudwatch')

# Environment variables
FILE_INVENTORY_TABLE = os.environ.get('FILE_INVENTORY_TABLE', 'agf-file-inventory-dev')
SYNC_RUNS_TABLE = os.environ.get('SYNC_RUNS_TABLE', 'agf-sync-runs-dev')
EXPERIMENTS_TABLE = os.environ.get('EXPERIMENTS_TABLE', 'agf-experiments-dev')
DATA_BUCKET = os.environ.get('DATA_BUCKET', 'agf-instrument-data')
ALERT_EMAIL = os.environ.get('ALERT_EMAIL', 'felix.meier@mq.edu.au')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')

# Initialize table references
file_inventory_table = dynamodb.Table(FILE_INVENTORY_TABLE)
sync_runs_table = dynamodb.Table(SYNC_RUNS_TABLE)
experiments_table = dynamodb.Table(EXPERIMENTS_TABLE)


def lambda_handler(event, context):
    """
    Main handler function triggered by CloudWatch Events (weekly schedule).
    Compares S3 files against ALL DynamoDB tables to find orphans.
    """
    print(f"Starting S3-DynamoDB reconciliation at {datetime.now(timezone.utc).isoformat()}")

    try:
        # Get ALL S3 keys (excluding only folders and .DS_Store)
        s3_keys = get_s3_keys()
        print(f"Found {len(s3_keys)} files in S3")

        # Get tracked keys from ALL DynamoDB tables
        dynamodb_keys = get_all_dynamodb_keys()
        print(f"Found {len(dynamodb_keys)} tracked files in DynamoDB (across all tables)")

        # Find discrepancies
        orphaned_in_s3 = s3_keys - dynamodb_keys  # Files in S3 but not tracked anywhere
        orphaned_in_db = dynamodb_keys - s3_keys  # Records pointing to non-existent files

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


def get_s3_keys() -> Set[str]:
    """
    Get all file keys from S3 bucket.
    Uses pagination to handle large buckets.

    Excludes only:
    - Folders (ending with /)
    - macOS artifacts (.DS_Store)
    """
    keys = set()
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=DATA_BUCKET, Prefix='raw/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('/') and '.DS_Store' not in key:
                keys.add(key)

    return keys


def get_all_dynamodb_keys() -> Set[str]:
    """
    Get all tracked S3 keys from ALL relevant DynamoDB tables.

    Combines keys from:
    - agf-file-inventory: s3_key field (data files)
    - agf-sync-runs: s3_key field (run.json files)
    - agf-experiments: s3_experiment_json_key field (experiment.json files)
    """
    all_keys = set()

    # 1. File inventory table (data files)
    file_keys = scan_table_for_keys(file_inventory_table, 's3_key')
    print(f"  - agf-file-inventory: {len(file_keys)} keys")
    all_keys.update(file_keys)

    # 2. Sync runs table (run.json files)
    run_keys = scan_table_for_keys(sync_runs_table, 's3_key')
    print(f"  - agf-sync-runs: {len(run_keys)} keys")
    all_keys.update(run_keys)

    # 3. Experiments table (experiment.json files)
    exp_keys = scan_table_for_keys(experiments_table, 's3_experiment_json_key')
    print(f"  - agf-experiments: {len(exp_keys)} keys")
    all_keys.update(exp_keys)

    return all_keys


def scan_table_for_keys(table, key_attribute: str) -> Set[str]:
    """
    Scan a DynamoDB table and extract all values of the specified key attribute.
    Uses pagination to handle large tables.
    """
    keys = set()
    scan_kwargs = {
        'ProjectionExpression': key_attribute,
    }

    done = False
    start_key = None

    while not done:
        if start_key:
            scan_kwargs['ExclusiveStartKey'] = start_key

        response = table.scan(**scan_kwargs)
        items = response.get('Items', [])

        for item in items:
            if key_attribute in item:
                keys.add(item[key_attribute])

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
        f"Total files in S3:          {len(s3_keys):,}",
        f"Total tracked in DynamoDB:  {len(dynamodb_keys):,}",
        "",
        f"Orphaned in S3 (untracked): {len(orphaned_in_s3):,}",
        f"Orphaned in DB (missing):   {len(orphaned_in_db):,}",
        "",
    ]

    if orphaned_in_s3:
        report_lines.extend([
            "FILES IN S3 NOT TRACKED IN ANY DYNAMODB TABLE (sample, max 20):",
            "-" * 40,
        ])
        for key in sorted(orphaned_in_s3)[:20]:
            report_lines.append(f"  - {key}")
        if len(orphaned_in_s3) > 20:
            report_lines.append(f"  ... and {len(orphaned_in_s3) - 20} more")
        report_lines.append("")

    if orphaned_in_db:
        report_lines.extend([
            "DYNAMODB RECORDS POINTING TO MISSING S3 FILES (sample, max 20):",
            "-" * 40,
        ])
        for key in sorted(orphaned_in_db)[:20]:
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
            "For untracked S3 files:",
            "  Option 1: Run backfill script to process these files",
            "            python3 scripts/backfill_s3_data.py --bucket agf-instrument-data",
            "  Option 2: Delete if they are test/junk files",
            "",
        ])

    if orphaned_in_db:
        report_lines.extend([
            "",
            "For orphaned DynamoDB records:",
            "  Option 1: Delete orphaned records (if S3 files were intentionally removed)",
            "  Option 2: Investigate - S3 files should be immutable",
            "",
        ])

    report_lines.append("=" * 60)

    return "\n".join(report_lines)


def publish_notification_metric(success: bool):
    """
    Publish CloudWatch metric for notification success/failure.
    """
    try:
        cloudwatch.put_metric_data(
            Namespace='AGF/Reconciliation',
            MetricData=[{
                'MetricName': 'NotificationSuccess' if success else 'NotificationFailure',
                'Value': 1,
                'Unit': 'Count'
            }]
        )
    except Exception as e:
        print(f"WARNING: Failed to publish CloudWatch metric: {e}")


def send_notification(report: str, orphaned_in_s3: Set[str], orphaned_in_db: Set[str]):
    """
    Send email notification with reconciliation report.
    Publishes CloudWatch metrics for notification success/failure.
    """
    subject = f"[AGF] S3-DynamoDB Reconciliation: {len(orphaned_in_s3) + len(orphaned_in_db)} discrepancies found"
    notification_sent = False

    if SNS_TOPIC_ARN:
        try:
            sns.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject[:100],
                Message=report
            )
            print(f"Notification sent via SNS to {SNS_TOPIC_ARN}")
            notification_sent = True
        except Exception as e:
            print(f"Failed to send SNS notification: {e}")

    # Fallback: Try SES if SNS failed or not configured
    if not notification_sent:
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
            notification_sent = True
        except Exception as e:
            print(f"Failed to send SES notification: {e}")
            print(f"Report:\n{report}")

    # Publish CloudWatch metric
    publish_notification_metric(notification_sent)

    return notification_sent


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
