#!/bin/bash
# AGF Data Infrastructure - Deployment Script for Felix
# Customized for Next.js 15.5.4 with 180 JSON files in S3

set -e

echo "=========================================="
echo "AGF Data Infrastructure Deployment"
echo "Next.js 15.5.4 | 180 JSON files to process"
echo "=========================================="
echo ""

# Configuration
ENVIRONMENT="dev"
AWS_REGION="ap-southeast-2"
S3_BUCKET="agf-instrument-data"
ALERT_EMAIL="${ALERT_EMAIL:-felix.meier@mq.edu.au}"
DYNAMODB_STACK="agf-dynamodb-stack-${ENVIRONMENT}"
LAMBDA_STACK="agf-lambda-stack-${ENVIRONMENT}"

echo "Checking prerequisites..."
echo ""

# Check Python 3
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is required but not installed"
    exit 1
fi
echo "✓ Python 3 found"

# Check boto3
if ! python3 -c "import boto3" 2>/dev/null; then
    echo "❌ boto3 is required. Install with: pip install boto3"
    exit 1
fi
echo "✓ boto3 found"

# Check AWS CLI
if ! command -v aws &> /dev/null; then
    echo "❌ AWS CLI not found. Install it first:"
    echo "   https://aws.amazon.com/cli/"
    exit 1
fi

echo "✓ AWS CLI found"

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo "❌ AWS credentials not configured"
    echo "Run: aws configure"
    exit 1
fi

AWS_ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
echo "✓ AWS Account: ${AWS_ACCOUNT}"
echo ""

# =============================================================================
# PHASE 1: Deploy DynamoDB Tables (3-5 minutes)
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 1: Deploying DynamoDB Tables"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

aws cloudformation deploy \
    --template-file agf-dynamodb-tables.yaml \
    --stack-name ${DYNAMODB_STACK} \
    --parameter-overrides EnvironmentName=${ENVIRONMENT} \
    --region ${AWS_REGION} \
    --capabilities CAPABILITY_NAMED_IAM

echo ""
echo "✓ DynamoDB tables deployed"

# Get table names
FILE_INVENTORY=$(aws cloudformation describe-stacks \
    --stack-name ${DYNAMODB_STACK} \
    --query "Stacks[0].Outputs[?OutputKey=='FileInventoryTableName'].OutputValue" \
    --output text --region ${AWS_REGION})

EXPERIMENTS=$(aws cloudformation describe-stacks \
    --stack-name ${DYNAMODB_STACK} \
    --query "Stacks[0].Outputs[?OutputKey=='ExperimentsTableName'].OutputValue" \
    --output text --region ${AWS_REGION})

echo "  Tables created:"
echo "    - ${FILE_INVENTORY}"
echo "    - ${EXPERIMENTS}"
echo ""

# =============================================================================
# PHASE 2: Populate Instrument Registry (30 seconds)
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 2: Populating Instrument Registry"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

if [ ! -f "Instrument_Inventory.csv" ]; then
    echo "❌ Instrument_Inventory.csv not found in current directory"
    echo "Please copy it here and re-run"
    exit 1
fi

python3 populate_instruments.py --environment ${ENVIRONMENT} --csv Instrument_Inventory.csv

echo ""
echo "✓ 31 instruments populated"
echo ""

# =============================================================================
# PHASE 3: Deploy Lambda Function (2-3 minutes)
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 3: Deploying Lambda Function"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Package Lambda
echo "Packaging Lambda function..."
rm -rf lambda_package lambda_deployment.zip 2>/dev/null || true
mkdir -p lambda_package
cp agf_ingestion_lambda.py lambda_package/index.py
cd lambda_package
zip -q -r ../lambda_deployment.zip .
cd ..
echo "✓ Lambda packaged"

# Create deployment bucket
DEPLOYMENT_BUCKET="${S3_BUCKET}-deployments"
if ! aws s3 ls s3://${DEPLOYMENT_BUCKET} &> /dev/null; then
    echo "Creating deployment bucket..."
    aws s3 mb s3://${DEPLOYMENT_BUCKET} --region ${AWS_REGION}
fi

# Upload package
echo "Uploading Lambda package..."
aws s3 cp lambda_deployment.zip s3://${DEPLOYMENT_BUCKET}/lambda/agf-ingestion-${ENVIRONMENT}.zip
echo "✓ Lambda package uploaded"

# Deploy Lambda stack
echo "Deploying Lambda stack..."
aws cloudformation deploy \
    --template-file agf-lambda-stack.yaml \
    --stack-name ${LAMBDA_STACK} \
    --parameter-overrides \
        EnvironmentName=${ENVIRONMENT} \
        S3BucketName=${S3_BUCKET} \
        DynamoDBStackName=${DYNAMODB_STACK} \
    --region ${AWS_REGION} \
    --capabilities CAPABILITY_NAMED_IAM

# Get Lambda function name
LAMBDA_FUNCTION=$(aws cloudformation describe-stacks \
    --stack-name ${LAMBDA_STACK} \
    --query "Stacks[0].Outputs[?OutputKey=='LambdaFunctionName'].OutputValue" \
    --output text --region ${AWS_REGION})

# Update Lambda code
echo "Updating Lambda code..."
aws lambda update-function-code \
    --function-name ${LAMBDA_FUNCTION} \
    --s3-bucket ${DEPLOYMENT_BUCKET} \
    --s3-key lambda/agf-ingestion-${ENVIRONMENT}.zip \
    --region ${AWS_REGION} \
    > /dev/null

echo ""
echo "✓ Lambda deployed: ${LAMBDA_FUNCTION}"
echo ""

# =============================================================================
# PHASE 3b: Deploy Zip Generator Lambda
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 3b: Deploying Zip Generator Lambda"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Package Zip Generator Lambda
echo "Packaging Zip Generator Lambda..."
rm -f zip_generator_deployment.zip 2>/dev/null || true
cp agf_zip_generator_lambda.py lambda_package/index.py
cd lambda_package
zip -q -r ../zip_generator_deployment.zip .
cd ..
echo "✓ Zip Generator Lambda packaged"

# Upload package
echo "Uploading Zip Generator Lambda package..."
aws s3 cp zip_generator_deployment.zip s3://${DEPLOYMENT_BUCKET}/lambda/agf-zip-generator-${ENVIRONMENT}.zip
echo "✓ Zip Generator Lambda package uploaded"

# Get Zip Generator Lambda function name
ZIP_LAMBDA_FUNCTION=$(aws cloudformation describe-stacks \
    --stack-name ${LAMBDA_STACK} \
    --query "Stacks[0].Outputs[?OutputKey=='ZipGeneratorLambdaName'].OutputValue" \
    --output text --region ${AWS_REGION} 2>/dev/null || echo "")

if [ -n "$ZIP_LAMBDA_FUNCTION" ] && [ "$ZIP_LAMBDA_FUNCTION" != "None" ]; then
    # Update Zip Generator Lambda code
    echo "Updating Zip Generator Lambda code..."
    aws lambda update-function-code \
        --function-name ${ZIP_LAMBDA_FUNCTION} \
        --s3-bucket ${DEPLOYMENT_BUCKET} \
        --s3-key lambda/agf-zip-generator-${ENVIRONMENT}.zip \
        --region ${AWS_REGION} \
        > /dev/null

    echo ""
    echo "✓ Zip Generator Lambda deployed: ${ZIP_LAMBDA_FUNCTION}"
else
    echo "ℹ️  Zip Generator Lambda not found - will be created on stack deploy"
fi
echo ""

# =============================================================================
# PHASE 4: Enable S3 EventBridge (10 seconds)
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 4: Enabling S3 EventBridge"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check if already enabled
EVENTBRIDGE_STATUS=$(aws s3api get-bucket-notification-configuration \
    --bucket ${S3_BUCKET} \
    --query 'EventBridgeConfiguration.Enabled' \
    --output text 2>/dev/null || echo "None")

if [ "$EVENTBRIDGE_STATUS" != "True" ]; then
    echo "Enabling EventBridge for S3..."
    aws s3api put-bucket-notification-configuration \
        --bucket ${S3_BUCKET} \
        --notification-configuration '{
            "EventBridgeConfiguration": {}
        }'
    echo "✓ EventBridge enabled"
else
    echo "✓ EventBridge already enabled"
fi
echo ""

# =============================================================================
# PHASE 5: Backfill Existing Data (15-20 minutes for 180 files)
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 5: Backfilling 180 JSON Files"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "This will process ~180 JSON files from S3 into DynamoDB"
echo "Estimated time: 15-20 minutes"
echo ""

read -p "Start backfill now? (y/n) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    python3 backfill_s3_data.py \
        --bucket ${S3_BUCKET} \
        --lambda-function ${LAMBDA_FUNCTION} \
        --max-workers 10
    
    echo ""
    echo "✓ Backfill complete"
else
    echo "⏭  Skipping backfill. Run manually later:"
    echo "   python3 backfill_s3_data.py --bucket ${S3_BUCKET} --lambda-function ${LAMBDA_FUNCTION}"
fi
echo ""

# =============================================================================
# PHASE 5b: Deploy Monitoring Alarms
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 5b: Deploying Monitoring Alarms"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

MONITORING_STACK="agf-monitoring-${ENVIRONMENT}"

aws cloudformation deploy \
    --template-file ../cloudformation/agf-monitoring-alarms.yaml \
    --stack-name ${MONITORING_STACK} \
    --parameter-overrides \
        EnvironmentName=${ENVIRONMENT} \
        AlertEmail=${ALERT_EMAIL} \
        LambdaFunctionName=${LAMBDA_FUNCTION} \
    --region ${AWS_REGION}

echo ""
echo "✓ Monitoring alarms deployed"
echo "  📧 Check your email for SNS subscription confirmation"
echo ""

# =============================================================================
# PHASE 5c: Deploy Reconciliation Lambda
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 5c: Deploying Reconciliation Lambda"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

RECONCILIATION_STACK="agf-reconciliation-${ENVIRONMENT}"

# Get SNS Topic ARN from monitoring stack
SNS_TOPIC_ARN=$(aws cloudformation describe-stacks \
    --stack-name ${MONITORING_STACK} \
    --query "Stacks[0].Outputs[?OutputKey=='AlertsTopicArn'].OutputValue" \
    --output text --region ${AWS_REGION} 2>/dev/null || echo "")

aws cloudformation deploy \
    --template-file ../cloudformation/agf-reconciliation-stack.yaml \
    --stack-name ${RECONCILIATION_STACK} \
    --capabilities CAPABILITY_NAMED_IAM \
    --parameter-overrides \
        EnvironmentName=${ENVIRONMENT} \
        AlertEmail=${ALERT_EMAIL} \
        DataBucketName=${S3_BUCKET} \
        AlertsTopicArn="${SNS_TOPIC_ARN}" \
    --region ${AWS_REGION}

# Get Reconciliation Lambda function name
RECONCILIATION_LAMBDA=$(aws cloudformation describe-stacks \
    --stack-name ${RECONCILIATION_STACK} \
    --query "Stacks[0].Outputs[?OutputKey=='ReconciliationLambdaName'].OutputValue" \
    --output text --region ${AWS_REGION})

# Package and update reconciliation Lambda code
echo "Packaging Reconciliation Lambda..."
rm -f reconciliation_deployment.zip 2>/dev/null || true
cp ../lambda/agf_reconciliation_lambda.py lambda_package/index.py
cd lambda_package
zip -q -r ../reconciliation_deployment.zip .
cd ..

aws s3 cp reconciliation_deployment.zip s3://${DEPLOYMENT_BUCKET}/lambda/agf-reconciliation-${ENVIRONMENT}.zip

aws lambda update-function-code \
    --function-name ${RECONCILIATION_LAMBDA} \
    --s3-bucket ${DEPLOYMENT_BUCKET} \
    --s3-key lambda/agf-reconciliation-${ENVIRONMENT}.zip \
    --region ${AWS_REGION} \
    > /dev/null

echo ""
echo "✓ Reconciliation Lambda deployed: ${RECONCILIATION_LAMBDA}"
echo "  📅 Runs weekly on Sundays at 2 AM AEST"
echo ""

# =============================================================================
# PHASE 6: Verify Deployment
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "PHASE 6: Verifying Deployment"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Check instruments table
INSTRUMENT_COUNT=$(aws dynamodb scan \
    --table-name agf-instruments-${ENVIRONMENT} \
    --select COUNT \
    --query 'Count' \
    --output text \
    --region ${AWS_REGION})

echo "✓ Instruments in DynamoDB: ${INSTRUMENT_COUNT}/31"

# Check if backfill ran
if [[ $REPLY =~ ^[Yy]$ ]]; then
    RUN_COUNT=$(aws dynamodb scan \
        --table-name agf-sync-runs-${ENVIRONMENT} \
        --select COUNT \
        --query 'Count' \
        --output text \
        --region ${AWS_REGION})
    
    echo "✓ Runs in DynamoDB: ${RUN_COUNT}"
    
    FILE_COUNT=$(aws dynamodb scan \
        --table-name agf-file-inventory-${ENVIRONMENT} \
        --select COUNT \
        --query 'Count' \
        --output text \
        --region ${AWS_REGION})
    
    echo "✓ Files in DynamoDB: ${FILE_COUNT}"
fi

echo ""

# =============================================================================
# Deployment Complete
# =============================================================================
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✅ AWS BACKEND DEPLOYMENT COMPLETE!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📋 Deployment Summary:"
echo "  DynamoDB Stack: ${DYNAMODB_STACK}"
echo "  Lambda Stack: ${LAMBDA_STACK}"
echo "  Monitoring Stack: ${MONITORING_STACK}"
echo "  Reconciliation Stack: ${RECONCILIATION_STACK}"
echo "  Lambda Function: ${LAMBDA_FUNCTION}"
echo "  S3 Bucket: ${S3_BUCKET}"
echo "  Region: ${AWS_REGION}"
echo ""
echo "🔍 Verify Data:"
echo "  aws dynamodb scan --table-name agf-instruments-${ENVIRONMENT} --limit 5"
echo "  aws dynamodb scan --table-name agf-sync-runs-${ENVIRONMENT} --limit 5"
echo ""
echo "📊 Monitor Lambda:"
echo "  aws logs tail /aws/lambda/${LAMBDA_FUNCTION} --follow"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "NEXT: Integrate Dashboard (Run setup-website.sh)"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
