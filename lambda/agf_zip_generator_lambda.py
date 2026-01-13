"""
AGF Zip Generator Lambda Function

Creates zip files on-demand from S3 objects for bulk download.
Invoked synchronously by the Next.js API when users request zip downloads.

Author: Claude Code
Version: 1.0
"""

import boto3
import zipfile
import json
import os
import tempfile
import shutil
from datetime import datetime

# Initialize AWS clients
s3 = boto3.client('s3')

# Configuration from environment variables
BUCKET = os.environ.get('S3_BUCKET', 'agf-instrument-data')
ZIP_PREFIX = 'downloads/zips/'

# Size limits
MAX_TOTAL_SIZE_BYTES = 5 * 1024 * 1024 * 1024  # 5GB limit
MAX_SINGLE_FILE_BYTES = 1 * 1024 * 1024 * 1024  # 1GB per file


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

        # Pre-validate all files (size check and accessibility)
        total_size = 0
        for s3_key in keys:
            try:
                head = s3.head_object(Bucket=BUCKET, Key=s3_key)
                file_size = head['ContentLength']

                if file_size > MAX_SINGLE_FILE_BYTES:
                    return {
                        'statusCode': 400,
                        'error': f'File too large: {s3_key} ({file_size / 1024 / 1024:.1f}MB > {MAX_SINGLE_FILE_BYTES / 1024 / 1024}MB limit)'
                    }

                total_size += file_size
            except Exception as e:
                return {'statusCode': 400, 'error': f'Cannot access file {s3_key}: {e}'}

        if total_size > MAX_TOTAL_SIZE_BYTES:
            return {
                'statusCode': 400,
                'error': f'Total size {total_size / 1024 / 1024 / 1024:.1f}GB exceeds {MAX_TOTAL_SIZE_BYTES / 1024 / 1024 / 1024}GB limit'
            }

        print(f"Pre-validation passed. Total size: {total_size / 1024 / 1024:.1f}MB")

        # Create temp directory for streaming files through /tmp
        # This avoids loading entire files into memory (OOM prevention)
        tmp_dir = tempfile.mkdtemp(dir='/tmp', prefix='agf_zip_')

        # Generate unique zip filename with timestamp
        timestamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
        base_name = zip_name.replace('.zip', '')
        zip_key = f"{ZIP_PREFIX}{base_name}-{timestamp}.zip"
        local_zip_path = os.path.join('/tmp', f"{base_name}-{timestamp}.zip")

        files_added = 0
        existing_names = set()  # O(1) lookup for duplicate detection

        try:
            with zipfile.ZipFile(local_zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
                for s3_key in keys:
                    print(f"Fetching: {s3_key}")

                    # Extract filename from S3 key
                    original_filename = s3_key.split('/')[-1]
                    filename = original_filename

                    # O(1) duplicate filename resolution
                    if filename in existing_names:
                        base, ext = os.path.splitext(filename)
                        counter = 1
                        while f"{base}_{counter}{ext}" in existing_names:
                            counter += 1
                        filename = f"{base}_{counter}{ext}"

                    existing_names.add(filename)

                    # Download file to temp directory (streaming, not memory)
                    tmp_file_path = os.path.join(tmp_dir, filename)

                    try:
                        s3.download_file(BUCKET, s3_key, tmp_file_path)
                    except Exception as e:
                        return {
                            'statusCode': 500,
                            'error': f'Failed to fetch {s3_key}: {str(e)}',
                            'message': 'Batch cancelled - no partial downloads. Please retry or contact admin.'
                        }

                    # Add to zip from temp file (streams from disk, not memory)
                    zf.write(tmp_file_path, filename)
                    files_added += 1
                    print(f"Added to zip: {filename}")

                    # Clean up temp file immediately to conserve /tmp space
                    os.unlink(tmp_file_path)

            # Get zip file size
            zip_size = os.path.getsize(local_zip_path)
            print(f"Uploading zip to S3: {zip_key} ({zip_size} bytes)")

            # Upload zip to S3 (streaming from file)
            s3.upload_file(
                local_zip_path,
                BUCKET,
                zip_key,
                ExtraArgs={
                    'ContentType': 'application/zip',
                    'Metadata': {
                        'files-count': str(files_added),
                        'created-by': 'agf-zip-generator'
                    }
                }
            )
        finally:
            # Clean up all temp files
            shutil.rmtree(tmp_dir, ignore_errors=True)
            if os.path.exists(local_zip_path):
                os.unlink(local_zip_path)

        print(f"Zip created successfully: {zip_key}")

        return {
            'statusCode': 200,
            'zipKey': zip_key,
            'fileCount': files_added,
            'zipSize': zip_size
        }

    except Exception as e:
        error_msg = f"Error creating zip: {str(e)}"
        print(error_msg)
        return {'statusCode': 500, 'error': error_msg}
