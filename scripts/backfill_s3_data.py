#!/usr/bin/env python3
"""
Backfill Existing S3 Data

Scans S3 bucket for existing run.json and experiment.json files,
then invokes Lambda function to process them into DynamoDB.

Author: Felix Meier
Version: 1.0
"""

import boto3
import argparse
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')


def find_json_files(bucket: str, prefix: str = 'raw/') -> list:
    """Find all run.json and experiment.json files in S3"""
    print(f"Scanning s3://{bucket}/{prefix} for metadata files...")
    
    json_files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('run.json') or key.endswith('experiment.json'):
                json_files.append({
                    'key': key,
                    'size': obj['Size'],
                    'last_modified': obj['LastModified']
                })
    
    return json_files


def create_s3_event(bucket: str, key: str) -> dict:
    """Create synthetic S3 event for Lambda invocation"""
    return {
        'Records': [
            {
                'eventVersion': '2.1',
                'eventSource': 'aws:s3',
                'awsRegion': 'ap-southeast-2',
                'eventTime': datetime.now().isoformat(),
                'eventName': 'ObjectCreated:Put',
                's3': {
                    'bucket': {
                        'name': bucket
                    },
                    'object': {
                        'key': key
                    }
                }
            }
        ]
    }


def invoke_lambda_for_file(lambda_function: str, bucket: str, key: str) -> dict:
    """Invoke Lambda function with S3 event for a single file"""
    event = create_s3_event(bucket, key)
    
    try:
        response = lambda_client.invoke(
            FunctionName=lambda_function,
            InvocationType='RequestResponse',  # Synchronous
            Payload=json.dumps(event)
        )
        
        result = json.loads(response['Payload'].read())
        
        if response['StatusCode'] == 200:
            return {'key': key, 'status': 'success', 'result': result}
        else:
            return {'key': key, 'status': 'error', 'error': result}
            
    except Exception as e:
        return {'key': key, 'status': 'error', 'error': str(e)}


def backfill_parallel(lambda_function: str, bucket: str, files: list, 
                     max_workers: int = 10) -> dict:
    """Backfill files in parallel using ThreadPoolExecutor"""
    
    print(f"\nProcessing {len(files)} files with {max_workers} workers...")
    
    results = {
        'success': 0,
        'error': 0,
        'total': len(files)
    }
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        futures = {
            executor.submit(invoke_lambda_for_file, lambda_function, bucket, f['key']): f 
            for f in files
        }
        
        # Process completed tasks
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            
            if result['status'] == 'success':
                results['success'] += 1
                print(f"  [{i}/{len(files)}] ✓ {result['key']}")
            else:
                results['error'] += 1
                print(f"  [{i}/{len(files)}] ✗ {result['key']}: {result.get('error', 'Unknown error')}")
            
            # Progress indicator
            if i % 10 == 0:
                print(f"  Progress: {i}/{len(files)} ({results['success']} success, {results['error']} errors)")
    
    return results


def main():
    parser = argparse.ArgumentParser(description='Backfill S3 metadata into DynamoDB')
    parser.add_argument('--bucket', required=True, 
                       help='S3 bucket name (e.g., agf-instrument-data)')
    parser.add_argument('--lambda-function', required=True,
                       help='Lambda function name (e.g., agf-data-ingestion-dev)')
    parser.add_argument('--prefix', default='raw/',
                       help='S3 prefix to scan (default: raw/)')
    parser.add_argument('--max-workers', type=int, default=10,
                       help='Number of parallel workers (default: 10)')
    parser.add_argument('--file-type', choices=['all', 'run', 'experiment'], default='all',
                       help='Which file types to process (default: all)')
    parser.add_argument('--dry-run', action='store_true',
                       help='List files without processing')
    args = parser.parse_args()
    
    print("=" * 60)
    print("AGF S3 Data Backfill")
    print("=" * 60)
    print(f"Bucket: s3://{args.bucket}/{args.prefix}")
    print(f"Lambda: {args.lambda_function}")
    print(f"Workers: {args.max_workers}")
    print(f"File type: {args.file_type}")
    print("=" * 60)
    
    # Find all JSON files
    all_files = find_json_files(args.bucket, args.prefix)
    
    # Filter by file type
    if args.file_type == 'run':
        files = [f for f in all_files if f['key'].endswith('run.json')]
    elif args.file_type == 'experiment':
        files = [f for f in all_files if f['key'].endswith('experiment.json')]
    else:
        files = all_files
    
    print(f"\nFound {len(files)} files to process:")
    run_count = sum(1 for f in files if f['key'].endswith('run.json'))
    exp_count = sum(1 for f in files if f['key'].endswith('experiment.json'))
    print(f"  - {run_count} run.json files")
    print(f"  - {exp_count} experiment.json files")
    
    if args.dry_run:
        print("\nDry run - listing files:")
        for f in files[:20]:  # Show first 20
            print(f"  {f['key']} ({f['size']} bytes)")
        if len(files) > 20:
            print(f"  ... and {len(files) - 20} more")
        return
    
    # Confirm before processing
    print("\nThis will invoke Lambda for each file.")
    confirm = input("Continue? (y/n): ")
    if confirm.lower() != 'y':
        print("Aborted.")
        return
    
    # Process files
    start_time = datetime.now()
    results = backfill_parallel(args.lambda_function, args.bucket, files, args.max_workers)
    duration = (datetime.now() - start_time).total_seconds()
    
    # Summary
    print("\n" + "=" * 60)
    print("Backfill Complete!")
    print("=" * 60)
    print(f"Total files: {results['total']}")
    print(f"✓ Success: {results['success']}")
    print(f"✗ Errors: {results['error']}")
    print(f"Duration: {duration:.1f} seconds")
    print(f"Throughput: {results['total'] / duration:.1f} files/second")
    print("=" * 60)
    
    # Verification
    print("\nVerify data in DynamoDB:")
    print(f"  aws dynamodb scan --table-name agf-file-inventory-dev --limit 10")
    print(f"  aws dynamodb scan --table-name agf-experiments-dev --limit 10")
    print(f"  aws dynamodb scan --table-name agf-sync-runs-dev --limit 10")


if __name__ == '__main__':
    main()
