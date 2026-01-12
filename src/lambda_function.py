from urllib.parse import urlparse
import logging
import boto3
import os

from utils.obfuscator_lib import obfuscate_data
# from .utils import obfuscate_data  # for utils/__init__.py

# Configure logging for CloudeWatch monitoring
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# ==========================================================
# CALLING PROCEDURE | LAMBDA HANDLER
# Handels the AWS Infrastructure and Saving procedure for demonstration purposes.
# ==========================================================
def lambda_handler(event, context):
    """
    AWS Lambda entry point that calls the obfuscator library.

    This function acts as a library module entry point. This demonstrates the calling procedure.
    It retrieves a file from ingestion S3 bucket, and calls the obfuscation tool which returns
    a byte stream that Lambda then saves to the destination S3 bucket for representation purposes.

    Args:
        event (dict): A JSON string (passed as a dict) containing:
            - 'file_to_obfuscate' (str): S3 URI (s3://ingestion_bucket/new_data/test_data.csv).
            - 'pii_fields' (list): List of column names to be masked.
            - 'primary_key' (str, optional): Primary key column name.
        context (object): AWS Lambda context object (unused).

    Returns:
        dict: Status of the obfuscation process: status code and success message.

    Raises:
        ValueError: If required (parameter) keys are missing from the event.
        Exception: General exceptions during lambda execution.
    """

    try:
        # Create S3 client for saving purposes
        s3_client = boto3.client("s3")

        # Get parameters from the EventBridge event (vagy környezeti változókból)
        s3_source_path = event.get("file_to_obfuscate")
        pii_fields = event.get("pii_fields")
        primary_key = event.get("primary_key")

        # Optional: EventBridge S3 PutObject event structure (get parameters from env var)
        if not s3_source_path and "detail" in event:
            ingestion_bucket = event["detail"]["bucket"]["name"]
            org_source_key = event["detail"]["object"]["key"]
            s3_source_path = f"s3://{ingestion_bucket}/{org_source_key}"
            if not pii_fields:
                pii_fields = os.environ.get("PII_FIELDS", "").split(",")
            if not primary_key:
                primary_key = os.environ.get("PRIMARY_KEY")

        # Error handling for missing parameters
        if not s3_source_path or not pii_fields:
            raise ValueError("Event must contain 'file_to_obfuscate' and 'pii_fields'")

        # 3. DEFINE org_source_key HERE (Move this outside of any conditional blocks)
        # Optional Logging for coudewatch clarity (ingestion_bucket, file_name)
        parsed_url = urlparse(s3_source_path)
        ingestion_bucket = parsed_url.netloc
        org_source_key = parsed_url.path.lstrip("/")  # <-- 'new_data/test_data.csv'
        file_name = s3_source_path.split("/")[-1]  # <-- 'test_data.csv'

        # log ingestion bucket, key and file name
        logger.info(
            f"Processing ingestion_bucket: {ingestion_bucket},"
            f"key: {org_source_key}, file_name: {file_name}"
        )

        # Destination bucket configuration from environment variables
        dest_bucket = os.environ.get("DESTINATION_BUCKET")
        logger.info(f"Destination bucket: {dest_bucket}")
        logger.info(f"pii_fields to obfuscate: {pii_fields}")

        # --- EXECUTE OBFUSCATOR LIBARY ---
        # Integration point: the handler calls the Obfuscator library.
        # (Default primary_key=None for auto-detect)
        logger.info(f"Call Obfuscator tool on file: {s3_source_path}")
        obfuscated_stream = obfuscate_data(s3_source_path, pii_fields, primary_key=None)

        # --- The LAMBDA HANDLER (calling procedure) SAVES THE DATA ---
        s3_client.put_object(
            Bucket=dest_bucket,
            Key=f"obfuscated/{org_source_key}",  # <-- 'new_data/test_data.csv'
            Body=obfuscated_stream.getvalue(),
        )

        logger.info(
            f"Successfully obfuscated and saved: s3://{dest_bucket}/obfuscated/{org_source_key}"
        )

        return {
            "status": 200,
            "message": f"File {org_source_key} successfully obfuscated and saved.",
        }

    except Exception as e:
        logger.error(f"Obfuscator Lambda Handler failed: {str(e)}")
        raise
