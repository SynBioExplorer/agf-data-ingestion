# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This submodule contains the data ingestion pipeline for the Australian Genome Foundry:
- **Lambda functions** for processing instrument metadata (run.json, experiment.json)
- **EventBridge rules** for S3 event triggers
- **Deployment scripts** for Lambda updates and data backfill

## Directory Structure

```
data-ingestion/
├── lambda/
│   ├── agf_ingestion_lambda.py      # Main metadata processor
│   ├── agf_zip_generator_lambda.py  # On-demand zip exports
│   ├── lambda-config.json           # Lambda function configurations
│   └── cloudformation/
│       └── agf-lambda-stack.yaml    # Lambda CloudFormation template
├── eventbridge/
│   └── event-rules.json             # EventBridge rule definitions
└── scripts/
    ├── backfill_s3_data.py          # Process existing S3 files
    └── DEPLOY_NOW.sh                # Full deployment script
```

## Common Commands

### Update Lambda Function
```bash
mkdir -p lambda_package
cp lambda/agf_ingestion_lambda.py lambda_package/index.py
cd lambda_package && zip -r ../lambda_deployment.zip . && cd ..
aws s3 cp lambda_deployment.zip s3://agf-instrument-data-deployments/lambda/agf_ingestion_lambda.zip
aws lambda update-function-code --function-name agf-data-ingestion-dev --s3-bucket agf-instrument-data-deployments --s3-key lambda/agf_ingestion_lambda.zip
```

### Backfill Historical Data
```bash
python3 scripts/backfill_s3_data.py --bucket agf-instrument-data --lambda-function agf-data-ingestion-dev --max-workers 10
```

### Monitor Lambda Logs
```bash
aws logs tail /aws/lambda/agf-data-ingestion-dev --follow
```

## Lambda Functions

| Function | Runtime | Memory | Timeout | Purpose |
|----------|---------|--------|---------|---------|
| agf-data-ingestion-dev | Python 3.11 | 512MB | 300s | Process run.json/experiment.json |
| agf-zip-generator-dev | Python 3.11 | 1024MB | 300s | Generate zip exports |

## Data Flow

```
S3 (*.json upload) → EventBridge → Lambda → DynamoDB
```
