"""
AGF Zip Generator Lambda Function

Creates zip files on-demand from S3 objects for bulk download.
Invoked synchronously by the Next.js API when users request zip downloads.

Author: Claude Code
Version: 1.0
"""

import boto3
import zipfile
import io
import json
import os
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')

# Configuration from environment variables
BUCKET = os.environ.get('S3_BUCKET', 'agf-instrument-data')
ZIP_PREFIX = 'downloads/zips/'


def lambda_handler(event, context):
    """
    Create a zip file from multiple S3 objects.

    Input event:
    {
        "keys": ["path/to/file1.csv", "path/to/file2.csv"],
        "zipName": "experiment-123.zip"  # optional
    }

    Returns:
    {
        "statusCode": 200,
        "zipKey": "downloads/zips/experiment-123-20241125-143022.zip",
        "fileCount": 5
    }
    """
    print(f"Received event: {json.dumps(event)}")

    try:
        keys = event.get('keys', [])
        zip_name = event.get('zipName', 'download.zip')

        if not keys:
            return {'statusCode': 400, 'error': 'No keys provided'}

        print(f"Creating zip with {len(keys)} files: {zip_name}")

        # Create zip in memory
        zip_buffer = io.BytesIO()
        files_added = 0
        errors = []

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for s3_key in keys:
                try:
                    print(f"Fetching: {s3_key}")
                    response = s3.get_object(Bucket=BUCKET, Key=s3_key)
                    file_content = response['Body'].read()

                    # Extract filename from S3 key
                    filename = s3_key.split('/')[-1]

                    # Handle duplicate filenames by adding a suffix
                    existing_names = [info.filename for info in zf.filelist]
                    if filename in existing_names:
                        base, ext = os.path.splitext(filename)
                        counter = 1
                        while f"{base}_{counter}{ext}" in existing_names:
                            counter += 1
                        filename = f"{base}_{counter}{ext}"

                    zf.writestr(filename, file_content)
                    files_added += 1
                    print(f"Added to zip: {filename}")

                except Exception as e:
                    error_msg = f"Error fetching {s3_key}: {str(e)}"
                    print(error_msg)
                    errors.append(error_msg)
                    continue

        if files_added == 0:
            return {
                'statusCode': 400,
                'error': 'No files could be added to zip',
                'details': errors
            }

        # Generate unique zip filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        base_name = zip_name.replace('.zip', '')
        zip_key = f"{ZIP_PREFIX}{base_name}-{timestamp}.zip"

        # Upload zip to S3
        zip_buffer.seek(0)
        zip_size = len(zip_buffer.getvalue())

        print(f"Uploading zip to S3: {zip_key} ({zip_size} bytes)")

        s3.put_object(
            Bucket=BUCKET,
            Key=zip_key,
            Body=zip_buffer.getvalue(),
            ContentType='application/zip',
            Metadata={
                'files-count': str(files_added),
                'created-by': 'agf-zip-generator'
            }
        )

        print(f"Zip created successfully: {zip_key}")

        return {
            'statusCode': 200,
            'zipKey': zip_key,
            'fileCount': files_added,
            'zipSize': zip_size,
            'errors': errors if errors else None
        }

    except Exception as e:
        error_msg = f"Error creating zip: {str(e)}"
        print(error_msg)
        return {'statusCode': 500, 'error': error_msg}
