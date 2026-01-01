import pytest
import boto3
import awswrangler as wr
import pandas as pd
import pandas.testing as pdt
import os
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


@pytest.fixture
def sample_csv_data():
    """Base csv sample data for tests."""
    headers = [
        "student_id",
        "name",
        "course",
        "cohort",
        "graduation_date",
        "email_address",
    ]
    data = [
        [
            1234,
            "John Smith",
            "Software",
            "2024-03-31",
            "2024-03-31",
            "j.smith@email.com",
        ],
        [
            5678,
            "Jane Doe",
            "Data Science",
            "2024-01-15",
            "2024-01-15",
            "j.doe@email.com",
        ],
    ]
    return pd.DataFrame(data, columns=headers)


@mock_aws
class TestObfuscator:
    def test_lambda_obfuscates_local_csv_file(self, s3_client):
        # ... start of the test ...
        start_time = time.time()

        # 1. Prepare test data
        headers = [
            "student_id",
            "name",
            "course",
            "cohort",
            "graduation_date",
            "email_address",
        ]
        test_input = [
            [
                1234,
                "John Smith",
                "Software",
                "2024-03-31",
                "2024-03-31",
                "j.smith@email.com",
            ],
            [
                5678,
                "Jane Doe",
                "Data Science",
                "2024-01-15",
                "2024-01-15",
                "j.doe@email.com",
            ],
        ]
        expected_output = [
            [1234, "*****", "Software", "2024-03-31", "2024-03-31", "*****"],
            [5678, "*****", "Data Science", "2024-01-15", "2024-01-15", "*****"],
        ]

        # 2. SETUP Mock Buckets and Env Variables
        source_key = "test_data.csv"
        source_bucket = "gdpr-source-bucket-test"
        dest_bucket = "gdpr-obfuscated-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket
        os.environ["PII_FIELDS"] = "name,email_address"

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input, columns=headers)
        wr.s3.to_csv(
            df=df_input, path=f"s3://{source_bucket}/{source_key}", index=False
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        lambda_handler(mock_event, None)

        # 4. ASSERT: Read the result from the dest_bucket
        result_df = wr.s3.read_csv(f"s3://{dest_bucket}/obfuscated/{source_key}")

        # ... end of the test ...
        end_time = time.time()

        expected_df = pd.DataFrame(expected_output, columns=headers)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)
        # Additional specific assertions
        assert result_df["name"][1] == "*****"
        assert result_df["student_id"][0] == 1234
        assert result_df["course"][1] == "Data Science"

        assert (end_time - start_time) < 60  # Test should complete within 60 seconds

    def test_lambda_obfuscates_local_json_file(self, s3_client):
        # 1. Prepare test data
        test_input = [
            {
                "student_id": 1234,
                "name": "John Smith",
                "course": "Software",
                "cohort": "2024-03-31",
                "graduation_date": "2024-03-31",
                "email_address": "j.smith@email.com",
            },
            {
                "student_id": 5678,
                "name": "Jane Doe",
                "course": "Data Science",
                "cohort": "2024-01-15",
                "graduation_date": "2024-01-15",
                "email_address": "j.doe@email.com",
            },
        ]
        expected_output = [
            {
                "student_id": 1234,
                "name": "*****",
                "course": "Software",
                "cohort": "2024-03-31",
                "graduation_date": "2024-03-31",
                "email_address": "*****",
            },
            {
                "student_id": 5678,
                "name": "*****",
                "course": "Data Science",
                "cohort": "2024-01-15",
                "graduation_date": "2024-01-15",
                "email_address": "*****",
            },
        ]

        # 2. SETUP Mock Buckets and Env Variables
        source_key = "test_data.json"
        source_bucket = "gdpr-source-bucket-test"
        dest_bucket = "gdpr-obfuscated-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket
        os.environ["PII_FIELDS"] = "name,email_address"

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input)
        wr.s3.to_json(
            df=df_input,
            path=f"s3://{source_bucket}/{source_key}",
            orient="records",
            lines=False,
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        lambda_handler(mock_event, None)

        # 4. ASSERT: Read the result from the dest_bucket
        result_df = wr.s3.read_json(
            f"s3://{dest_bucket}/obfuscated/{source_key}", orient="records", lines=False
        )

        expected_df = pd.DataFrame(expected_output)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)
        # Additional specific assertions
        assert result_df["name"][1] == "*****"
        assert result_df["student_id"][0] == 1234
        assert result_df["course"][1] == "Data Science"

    def test_lambda_obfuscates_local_parquet_file(self, s3_client, sample_csv_data):
        # SETUP - Buckets and env variables
        source_bucket = "test-source-bucket"
        dest_bucket = "test-dest-bucket"
        source_key = "test_data.parquet"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket
        os.environ["PII_FIELDS"] = "name,email_address"

        # SEED - Upload sample_csv_data in Parquet format
        source_path = f"s3://{source_bucket}/{source_key}"
        wr.s3.to_parquet(df=sample_csv_data, path=source_path, index=False)

        # ACT - Call the lambda_handler with EventBridge style event
        event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }
        lambda_handler(event, None)

        # 4. ASSERT - Evaluate the obfuscated Parquet file
        result_df = wr.s3.read_parquet(f"s3://{dest_bucket}/obfuscated/{source_key}")
        expected_df = sample_csv_data.copy()
        expected_df["name"] = "*****"
        expected_df["email_address"] = "*****"

        # dtype alignment
        result_df = result_df.astype(expected_df.dtypes)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)

        # Individual assertions
        assert result_df["name"].iloc[0] == "*****"
        assert result_df["student_id"].iloc[0] == 1234
        assert result_df["course"].iloc[1] == "Data Science"
        assert result_df["email_address"].iloc[1] == "*****"

    def test_lambda_raises_error_if_file_not_found(self, s3_client):
        # Setup: Only create S3 bucket, but NOT place file in it.
        source_bucket = "gdpr-source-bucket-test"
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
        source_bucket = "gdpr-source-bucket-test"
        dest_bucket = "gdpr-dest-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket
        os.environ["PII_FIELDS"] = "name,email_address"

        # Seed: Upload a corrupted CSV file with binearis/incorrect content
        corrupted_content = b"\xff\xfe\xfd\x12"

        source_key = "corrupted.csv"
        s3_client.put_object(
            Bucket=source_bucket,
            Key=source_key,
            Body=corrupted_content,
        )

        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        # Assert: Expect an exception due to incorrect file content
        with pytest.raises(Exception) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the exception message contains
        assert "codec can't decode byte" in str(excinfo.value)

    def test_lambda_raises_error_no_pii_fields_obfuscated(self, s3_client):
        # Setup Data
        headers = ["student_id", "course", "cohort", "graduation_date"]
        test_input = [
            [1234, "Software", "2024-03-31", "2024-03-31"],
            [5678, "Data Science", "2024-01-15", "2024-01-15"],
        ]

        # Setup
        source_key = "no_pii_fields.csv"
        source_bucket = "source-bucket-test"
        dest_bucket = "dest-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket
        os.environ["PII_FIELDS"] = "name,email_address"

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input, columns=headers)
        wr.s3.to_csv(
            df=df_input, path=f"s3://{source_bucket}/{source_key}", index=False
        )

        df_input = pd.DataFrame(test_input, columns=headers)
        s3_client.put_object(
            Bucket=source_bucket,
            Key=source_key,
            Body=df_input.to_csv(index=False).encode("utf-8"),
        )

        mock_event = {
            "detail": {"bucket": {"name": source_bucket}, "object": {"key": source_key}}
        }

        # Assert: Expect an exception due to no PII fields found
        with pytest.raises(Exception) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the exception message contains
        assert "No PII columns found to obfuscate." in str(excinfo.value)

    def test_obfuscate_data_unsupported_format_raises_error(self, s3_client):
        # Setup S3 buckets and env variables
        source_bucket = "gdpr-source-bucket-test"
        dest_bucket = "gdpr-destination-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket
        os.environ["PII_FIELDS"] = "name,email_address"

        # SEED: UpLoad an UNSUPPORTED file from your local folder to S3
        source_key = "dummy.txt"

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
