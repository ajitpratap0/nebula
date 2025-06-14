# S3 Destination Connector Example

This example demonstrates how to use the S3 destination connector to upload data from local files to Amazon S3.

## Prerequisites

1. AWS Account with S3 access
2. AWS credentials configured (one of the following):
   - Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
   - AWS credentials file: `~/.aws/credentials`
   - IAM role (for EC2 instances)
   - AWS SSO

## Example Usage

### CSV to S3 (CSV format)

```bash
# Upload CSV data to S3 bucket in CSV format
./nebula pipeline csv s3 \
  --source-path data.csv \
  --dest-path s3://my-bucket/data/ \
  --batch-size 1000
```

### JSON to S3 (JSON Lines format)

```bash
# Upload JSON data to S3 bucket in JSONL format
./nebula pipeline json s3 \
  --source-path data.json \
  --dest-path s3://my-bucket/json-data/ \
  --format lines \
  --batch-size 500
```

## Configuration Options

The S3 destination supports the following configuration:

### Required Configuration
- `bucket`: S3 bucket name
- `region`: AWS region (e.g., us-east-1)

### Optional Configuration
- `prefix`: Object key prefix for uploaded files
- `aws_access_key_id`: AWS access key (if not using IAM roles)
- `aws_secret_access_key`: AWS secret key (if not using IAM roles)
- `format`: Output format (`csv`, `json`, `jsonl`)
- `compression`: Compression type (`none`, `gzip`)
- `batch_size`: Records per file (default: 1000)
- `max_file_size`: Maximum file size in bytes (default: 100MB)
- `flush_interval`: Time interval to flush files (default: 5m)

### Advanced Example with Custom Configuration

```bash
# Advanced S3 upload with custom settings
./nebula pipeline postgresql s3 \
  --source-config '{
    "connection_string": "postgres://user:password@localhost:5432/db",
    "table": "users"
  }' \
  --dest-config '{
    "bucket": "analytics-data",
    "region": "us-west-2", 
    "prefix": "users/",
    "format": "jsonl",
    "compression": "gzip",
    "batch_size": 5000,
    "flush_interval": "10m"
  }'
```

## Output Structure

Files uploaded to S3 follow this structure:

```
s3://bucket/prefix/year=YYYY/month=MM/day=DD/hour=HH/data_YYYYMMDD_HHMMSS_XXXXXX.format[.gz]
```

Example:
```
s3://analytics-data/users/year=2024/month=01/day=15/hour=14/data_20240115_143052_123456.jsonl.gz
```

## Features

- **Automatic Partitioning**: Files are automatically partitioned by date (year/month/day/hour)
- **Batching**: Records are batched for efficient uploads
- **Compression**: Optional gzip compression
- **Large File Support**: Automatic handling of large files
- **Error Recovery**: Circuit breaker and retry logic
- **Health Monitoring**: Built-in health checks and metrics
- **Multiple Formats**: Support for CSV, JSON, and JSON Lines formats

## Monitoring

The S3 destination provides comprehensive metrics:

- Records written
- Bytes uploaded
- Files uploaded
- Current batch size
- Last flush time
- Upload success/failure rates

## Error Handling

The connector includes robust error handling:

- **Circuit Breaker**: Automatic failure detection and recovery
- **Rate Limiting**: Prevents overwhelming S3 API
- **Retry Logic**: Automatic retry of failed uploads
- **Health Checks**: Continuous monitoring of S3 connectivity

## Security Best Practices

1. **Use IAM Roles**: Prefer IAM roles over access keys when possible
2. **Least Privilege**: Grant only necessary S3 permissions
3. **Encryption**: Enable S3 bucket encryption
4. **Monitoring**: Monitor S3 access logs and CloudTrail events

## Required S3 Permissions

Your AWS credentials need the following S3 permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:PutObjectAcl",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket-name",
        "arn:aws:s3:::your-bucket-name/*"
      ]
    }
  ]
}
```

## Troubleshooting

### Common Issues

1. **Access Denied**: Check AWS credentials and S3 permissions
2. **Invalid Region**: Ensure the region matches your bucket's region
3. **Bucket Not Found**: Verify bucket name and region
4. **Network Issues**: Check internet connectivity and security groups

### Debug Mode

Enable debug logging for troubleshooting:

```bash
export NEBULA_LOG_LEVEL=debug
./nebula pipeline csv s3 --source-path data.csv --dest-path s3://bucket/
```