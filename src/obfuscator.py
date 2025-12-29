import awswrangler as wr
from io import BytesIO
import logging
import boto3
import os

# Configure logging for CloudeWatch monitoring
logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Function to obfuscate PII data in the relevant file format stored in S3
def lambda_handler(event, context):
    """
    AWS Lambda handler to obfuscate PII data in a CSV file stored in S3.
    Lambda triggered by EventBridge when a new file is created in S3.

    Args:
        event (dict): Event data containing S3 bucket/key and PII fields.
        context (object): Lambda context object.

    Returns:
        dict: Status of the obfuscation process.
    """

    try:
        # 1a. Create S3 client
        s3_client = boto3.client("s3")
        logger.info("created s3 client")
        # 1b. Get configuration from environment variables
        destination_bucket = os.environ.get("DESTINATION_BUCKET")
        # PII fields to obfuscate from enviorment variables
        pii_fields = os.environ.get("PII_FIELDS", "").split(",")

        # 2. Parse S3 file information from the EventBridge event structure
        source_bucket = event["detail"]["bucket"]["name"]
        source_key = event["detail"]["object"]["key"]
        s3_source_path = f"s3://{source_bucket}/{source_key}"

        logger.info(f"Triggered for file: {source_key} " f"in bucket: {source_bucket}")
        logger.info(f"PII columns to obfuscate: {pii_fields}")

        # 3. Execute obfuscation function | anonymizator tool
        obfuscated_stream = obfuscate_data(s3_source_path, pii_fields)

        # 4. Save the resulting stream to the destination bucket
        # The 'calling procedure' handles the saving path and naming convention
        s3_client.put_object(
            Bucket=destination_bucket,
            Key=f"obfuscated/{source_key}",
            Body=obfuscated_stream.getvalue(),
        )

        # 5. Return success status
        return {
            "status": 200,
            "message": f"File {source_key} successfully obfuscated and saved.",
        }

    # Error handling
    except Exception as e:
        logger.error(f"Obfuscator Lambda Handler failed: {str(e)}")
        raise


# Obfuscation - Anonymization function to mask PII values
def obfuscate_data(s3_source_path, pii_fields):
    """
    Obfuscates | Anonymizate | Mask PII fields in a file and returns a byte stream.
    MVP only CSV files, Extended detects file format for json, parquet

    Args:
        s3_source_path (str): The S3 URI of the source file (e.g., s3://source_bucket/data.csv).
        pii_fields (list): List of the column names to be obfuscated.

    Returns:
        BytesIO: A byte stream object containing the obfuscated CSV data.
    """

    try:
        # Determine file extension
        extension = s3_source_path.split(".")[-1].lower()

        # 1. Load data based on format
        if extension == "csv":
            df = wr.s3.read_csv(s3_source_path)
        elif extension == "json":
            df = wr.s3.read_json(s3_source_path, orient="records")
        elif extension == "parquet":
            df = wr.s3.read_parquet(s3_source_path)
        else:
            # This should not be reached technically if EventBridge filter works
            logger.error(f"Unsupported format: {extension} from: {s3_source_path}")
            raise Exception(f"Unsupported format: {extension}")

        logger.info(f"Successfully read {extension} from: {s3_source_path}")

        # 2. Process Obfuscation - mask PII columns by replacing with '*****'
        count_obfuscated_col = 0
        existing_pii = []
        for col in pii_fields:
            if col in df.columns:
                df[col] = "*****"  # Obfuscation by masking with asterisks
                existing_pii.append(col)
                count_obfuscated_col += 1
                logger.info(f"obfuscated column: {col}")
        if count_obfuscated_col == 0:
            logger.warning("No PII columns found to obfuscate.")

        # 3. Convert back to Byte Stream
        output_buffer = BytesIO()

        if extension == "csv":
            df.to_csv(output_buffer, index=False)
        elif extension == "json":
            import json

            json_string = df.to_json(orient="records")
            parsed = json.loads(json_string)
            pretty_json = json.dumps(parsed, indent=2)
            output_buffer.write(pretty_json.encode("utf-8"))
        elif extension == "parquet":
            df.to_parquet(output_buffer, index=False)

        output_buffer.seek(0)  # reset buffer position to the beginning
        logger.info(f"Successfully obfuscated {len(existing_pii)} fields.")

        # 4. Return the byte stream
        return output_buffer

    except Exception as e:
        logger.error(f"Error in obfuscate_data: {str(e)}")
        raise
