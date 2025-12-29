import pytest
import boto3
import awswrangler as wr
import pandas as pd
import os
import hcl2
from moto import mock_aws
from src.obfuscator import lambda_handler
import time


@pytest.fixture(scope="function")
def s3_client():
    """Yields a mocked S3 client."""
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


# Helper function to read PII fields from terraform.tfvars
def get_pii_from_terraform():
    with open("terraform/terraform.tfvars", "r") as f:
        tf_vars = hcl2.load(f)
        # 'pii_fields' is a list in the tfvars file
        return ",".join(tf_vars["pii_fields"])


@mock_aws
class TestObfuscator:
    def test_lambda_obfuscates_local_csv_file(self, s3_client):
        # ... start of the test ...
        start_time = time.time()

        # 1a. SETUP: Mock Buckets
        source_bucket = "gdpr-source-data-bucket-test"
        dest_bucket = "gdpr-obfuscated-data-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # 1b. Configure Environment variables for the Lambda function
        os.environ["DESTINATION_BUCKET"] = dest_bucket
        # Configure PII fields for dummy.csv has 'name' and 'email_address'
        os.environ["PII_FIELDS"] = get_pii_from_terraform()

        # 2. SEED: Load your ACTUAL local file and upload it to the mock S3
        source_key = "dummy.csv"
        local_file_path = f"data/raw/{source_key}"

        # Ensure the directory exists to avoid FileNotFoundError during the test
        if not os.path.exists(local_file_path):
            pytest.fail("Local test file not found.")

        df_local = pd.read_csv(local_file_path)
        wr.s3.to_csv(df_local, f"s3://{source_bucket}/{source_key}", index=False)

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        lambda_handler(mock_event, None)
        # ... end of the test ...
        end_time = time.time()

        # 4. ASSERT: Read the result from the destination bucket
        result_df = wr.s3.read_csv(f"s3://{dest_bucket}/obfuscated/{source_key}")

        # 5. If not exsist, create the local "obfuscated" folder, and save the result there
        output_dir = "data/obfuscated"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save the result locally for manual inspection
        output_path = os.path.join(output_dir, "obfuscated_result.csv")
        result_df.to_csv(output_path, index=False)

        print(f"\nSuccess! Find the Obfuscated file here: {output_path}")

        # 6. ASSERT: Validate that PII fields are obfuscated
        # Check that PII fields are masked and the columns defined
        # in PII_FIELDS env variable as written in terraform.tfvars
        # hardcoded here for clarity:["name", "email_address"]
        for col in ["name", "email_address"]:
            if col in result_df.columns:
                assert all(
                    result_df[col] == "*****"
                ), f"Column {col} was not obfuscated"

        # Check that one non-PII field (e.g., student_id) is still the same as original
        if "student_id" in result_df.columns:
            assert result_df["student_id"][0] == df_local["student_id"][0]

        assert (end_time - start_time) < 60  # Test should complete within 60 seconds

    def test_lambda_obfuscates_local_json_file(self, s3_client):
        # 1a. SETUP: Mock Buckets
        source_bucket = "gdpr-source-data-bucket-test"
        dest_bucket = "gdpr-obfuscated-data-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # 1b. Configure Environment variables for the Lambda function
        os.environ["DESTINATION_BUCKET"] = dest_bucket
        # Configure PII fields for dummy.csv has 'name' and 'email_address'
        os.environ["PII_FIELDS"] = get_pii_from_terraform()

        # 2. SEED: Load your ACTUAL local file and upload it to the mock S3
        source_key = "dummy.json"
        local_file_path = f"data/raw/{source_key}"

        # Ensure the directory exists to avoid FileNotFoundError during the test
        if not os.path.exists(local_file_path):
            pytest.fail("Local test file not found.")

        df_local = pd.read_json(local_file_path)
        wr.s3.to_json(
            df_local,
            f"s3://{source_bucket}/{source_key}",
            orient="records",
            lines=False,
            index=False,
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        lambda_handler(mock_event, None)

        # 4. ASSERT: Read the result from the destination bucket
        result_df = wr.s3.read_json(
            f"s3://{dest_bucket}/obfuscated/{source_key}", orient="records", lines=False
        )

        # 5. If not exsist, create the local "obfuscated" folder, and save the result there
        output_dir = "data/obfuscated"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save the result locally for manual inspection
        output_path = os.path.join(output_dir, "obfuscated_result.json")
        result_df.to_json(output_path, orient="records", indent=2, index=False)

        print(f"\nSuccess! Find the Obfuscated file here: {output_path}")

        # 6. ASSERT: Validate that PII fields are obfuscated
        # Check that PII fields are masked and the columns defined
        # in PII_FIELDS env variable as written in terraform.tfvars
        # hardcoded here for clarity:["name", "email_address"]
        for col in ["name", "email_address"]:
            if col in result_df.columns:
                assert all(
                    result_df[col] == "*****"
                ), f"Column {col} was not obfuscated"

        # Check that one non-PII field (e.g., student_id) is still the same as the original
        if "student_id" in result_df.columns:
            assert result_df["student_id"][0] == df_local["student_id"][0]

    def test_lambda_obfuscates_local_parquet_file(self, s3_client):
        # 1a. SETUP: Mock Buckets
        source_bucket = "gdpr-source-data-bucket-test"
        dest_bucket = "gdpr-obfuscated-data-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # 1b. Configure Environment variables for the Lambda function
        os.environ["DESTINATION_BUCKET"] = dest_bucket
        # Configure PII fields for dummy.csv has 'name' and 'email_address'
        os.environ["PII_FIELDS"] = get_pii_from_terraform()

        # 2. SEED: Load your ACTUAL local file and upload it to the mock S3
        source_key = "dummy.parquet"
        local_file_path = f"data/raw/{source_key}"

        # Ensure the directory exists to avoid FileNotFoundError during the test
        if not os.path.exists(local_file_path):
            pytest.fail("Local test file not found.")

        df_local = pd.read_parquet(local_file_path)
        wr.s3.to_parquet(df_local, f"s3://{source_bucket}/{source_key}", index=False)

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        lambda_handler(mock_event, None)

        # 4. ASSERT: Read the result from the destination bucket
        result_df = wr.s3.read_parquet(f"s3://{dest_bucket}/obfuscated/{source_key}")

        # 5. If not exsist, create the local "obfuscated" folder, and save the result there
        output_dir = "data/obfuscated"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        # Save the result locally for manual inspection
        output_path = os.path.join(output_dir, "obfuscated_result.parquet")
        result_df.to_parquet(output_path, index=False)

        print(f"\nSuccess! Find the Obfuscated file here: {output_path}")

        # 6. ASSERT: Validate that PII fields are obfuscated
        # Check that PII fields are masked and the columns defined
        # in PII_FIELDS env variable as written in terraform.tfvars
        # hardcoded here for clarity:["name", "email_address"]
        for col in ["name", "email_address"]:
            if col in result_df.columns:
                assert all(
                    result_df[col] == "*****"
                ), f"Column {col} was not obfuscated"

        # Check that one non-PII field (e.g., student_id) is still the same as the original
        if "student_id" in result_df.columns:
            assert result_df["student_id"][0] == df_local["student_id"][0]

    def test_lambda_raises_error_if_file_not_found(self, s3_client):
        # Setup: Only create S3 bucket, but NOT place file in it.
        source_bucket = "gdpr-source-data-bucket-test"
        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_event = {
            "detail": {
                "bucket": {"name": source_bucket},
                "object": {"key": "non_existent_file.csv"},
            }
        }

        # Assert: Expect an exception due to missing file
        with pytest.raises(Exception) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the exception message contains "NoSuchKey"
        assert "No files Found" in str(excinfo.value)

    def test_lambda_raises_error_on_corrupted_csv(self, s3_client):
        # Setup
        source_bucket = "gdpr-source-data-bucket-test"
        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Seed: Upload a corrupted CSV file with incorrect content
        source_key = "corrupted.csv"
        s3_client.put_object(
            Bucket=source_bucket,
            Key=source_key,
            Body="This is not a valid CSV content, bc it lacks structure!",
        )

        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        # Assert: Expect an exception due to incorrect file content
        with pytest.raises(Exception):
            lambda_handler(mock_event, None)

    def test_obfuscate_data_unsupported_format_raises_error(self, s3_client):
        # 1a. Setup S3 buckets
        source_bucket = "gdpr-source-data-bucket-test"
        dest_bucket = "gdpr-destination-data-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # 1b. Configure Environment variables
        os.environ["DESTINATION_BUCKET"] = dest_bucket
        os.environ["PII_FIELDS"] = get_pii_from_terraform()

        # 2. SEED: UpLoad your an UNSUPPORTED file from your local folder to S3
        source_key = "dummy.txt"
        local_file_path = f"data/raw/{source_key}"

        # Ensure the directory exists to avoid FileNotFoundError during the test
        if not os.path.exists(local_file_path):
            pytest.fail(
                f"Local test file not found at {local_file_path}. Please create it first."
            )

        s3_client.put_object(
            Bucket=source_bucket,
            Key=source_key,
            Body="This is a txt file, not supported format.",
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        # Assert: Expect an exception due to incorrect file content
        with pytest.raises(Exception) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the exception message contains "Unsupported format: txt"
        error_msg = str(excinfo.value)
        assert "Unsupported format: txt" in error_msg

    def test_lambda_handler_error_logging(self, caplog):
        """
        Test that the lambda_handler logs an error when given a bad event structure.
        """

        bad_event = {"invalid": "structure"}

        with pytest.raises(Exception):
            lambda_handler(bad_event, None)

        # Check that the error was logged
        assert "Obfuscator Lambda Handler failed" in caplog.text
