import pytest
import boto3
import awswrangler as wr
import pandas as pd
import pandas.testing as pdt
import os
from moto import mock_aws
from src.lambda_function import lambda_handler
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
    def test_lambda_obfuscates_csv_file(self, s3_client):
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
                12 - 34 - 33,
                "John Smith",
                "Software",
                "2024-03-31",
                "2024-03-31",
                "j.smith@email.com",
            ],
            [
                56 - 78 - 22,
                "Jane Doe",
                "Data Science",
                "2024-01-15",
                "2024-01-15",
                "j.doe@email.com",
            ],
        ]
        expected_output = [
            [12 - 34 - 33, "***", "Software", "2024-03-31", "2024-03-31", "***"],
            [56 - 78 - 22, "***", "Data Science", "2024-01-15", "2024-01-15", "***"],
        ]

        # 2. SETUP Mock Buckets and Env Variables
        file_name = "test_data.csv"
        source_bucket = "gdpr-ingestion-bucket-test"
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

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input, columns=headers)
        wr.s3.to_csv(
            df=df_input, path=f"s3://{source_bucket}/new_data/{file_name}", index=False
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "file_to_obfuscate": "s3://gdpr-ingestion-bucket-test/new_data/test_data.csv",
            "pii_fields": ["name", "email_address"],
        }

        lambda_handler(mock_event, None)

        # 4. ASSERT: Read the result from the dest_bucket
        result_df = wr.s3.read_csv(
            f"s3://{dest_bucket}/obfuscated/new_data/{file_name}"
        )

        # ... end of the test ...
        end_time = time.time()

        expected_df = pd.DataFrame(expected_output, columns=headers)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)
        # Additional specific assertions
        assert result_df["name"][1] == "***"
        assert result_df["student_id"][0] == 12 - 34 - 33
        assert result_df["course"][1] == "Data Science"

        # Test should complete within 60 seconds
        assert (end_time - start_time) < 60

    def test_lambda_obfuscates_json_file(self, s3_client):
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
                "name": "***",
                "course": "Software",
                "cohort": "2024-03-31",
                "graduation_date": "2024-03-31",
                "email_address": "***",
            },
            {
                "student_id": 5678,
                "name": "***",
                "course": "Data Science",
                "cohort": "2024-01-15",
                "graduation_date": "2024-01-15",
                "email_address": "***",
            },
        ]

        # 2. SETUP Mock Buckets and Env Variables
        file_name = "test_data.json"
        source_bucket = "gdpr-ingestion-bucket-test"
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

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input)
        wr.s3.to_json(
            df=df_input,
            path=f"s3://{source_bucket}/new_data/{file_name}",
            orient="records",
            lines=False,
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "file_to_obfuscate": "s3://gdpr-ingestion-bucket-test/new_data/test_data.json",
            "pii_fields": ["name", "email_address"],
        }

        lambda_handler(mock_event, None)

        # 4. ASSERT: Read the result from the dest_bucket
        result_df = wr.s3.read_json(
            f"s3://{dest_bucket}/obfuscated/new_data/{file_name}",
            orient="records",
            lines=False,
        )

        expected_df = pd.DataFrame(expected_output)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)
        # Additional specific assertions
        assert result_df["name"][1] == "***"
        assert result_df["student_id"][0] == 1234
        assert result_df["course"][1] == "Data Science"

    def test_lambda_obfuscates_parquet_file(self, s3_client, sample_csv_data):
        # SETUP - Buckets and env variables
        source_bucket = "test-source-bucket"
        dest_bucket = "test-dest-bucket"
        file_name = "test_data.parquet"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket

        # SEED - Upload sample_csv_data in Parquet format
        source_path = f"s3://{source_bucket}/new_data/{file_name}"
        wr.s3.to_parquet(df=sample_csv_data, path=source_path, index=False)

        # ACT - Call the lambda_handler with EventBridge style event
        mock_event = {
            "file_to_obfuscate": "s3://test-source-bucket/new_data/test_data.parquet",
            "pii_fields": ["name", "email_address"],
        }
        lambda_handler(mock_event, None)

        # 4. ASSERT - Evaluate the obfuscated Parquet file
        result_df = wr.s3.read_parquet(
            f"s3://{dest_bucket}/obfuscated/new_data/{file_name}"
        )
        expected_df = sample_csv_data.copy()
        expected_df["name"] = "***"
        expected_df["email_address"] = "***"

        # dtype alignment
        result_df = result_df.astype(expected_df.dtypes)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)

        # Individual assertions
        assert result_df["name"].iloc[0] == "***"
        assert result_df["student_id"].iloc[0] == 1234
        assert result_df["course"].iloc[1] == "Data Science"
        assert result_df["email_address"].iloc[1] == "***"

    def test_lambda_handler_handles_eventbridge_format(self, s3_client):
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
            [1234, "***", "Software", "2024-03-31", "2024-03-31", "***"],
            [5678, "***", "Data Science", "2024-01-15", "2024-01-15", "***"],
        ]

        # 2. Setup
        file_name = "test.csv"
        source_bucket = "source-bucket-test"
        os.environ["DESTINATION_BUCKET"] = "dest-bucket"
        os.environ["PII_FIELDS"] = "name,email_address"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket="dest-bucket",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        # Create input DataFrame
        df_input = pd.DataFrame(test_input, columns=headers)
        # Seed - Upload a test CSV file to the source bucket
        wr.s3.to_csv(
            df=df_input, path=f"s3://{source_bucket}/new_data/{file_name}", index=False
        )

        # Create EventBridge style event
        eventbridge_event = {
            "detail": {
                "bucket": {"name": source_bucket},
                "object": {"key": "new_data/test.csv"},
            }
        }

        # 3. ACT
        lambda_handler(eventbridge_event, None)

        # ... end of the test ...
        end_time = time.time()

        # 4. ASSERT: Read the result from the dest_bucket
        result_df = wr.s3.read_csv("s3://dest-bucket/obfuscated/new_data/test.csv")
        assert result_df["name"][0] == "***"

        expected_df = pd.DataFrame(expected_output, columns=headers)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)
        # Additional specific assertions
        assert result_df["name"][1] == "***"
        assert result_df["student_id"][0] == 1234
        assert result_df["course"][1] == "Data Science"
        # time performance check
        assert (end_time - start_time) < 60

    def test_lambda_raises_error_if_file_not_found(self, s3_client):
        # Setup: Only create S3 bucket, but NOT place file in it.
        source_bucket = "source-bucket-test"
        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        mock_event = {
            "file_to_obfuscate": "s3://source-bucket-test/new_data/non_existent_file.csv",
            "pii_fields": ["name", "email_address"],
        }

        # Assert: Expect an exception due to missing file
        with pytest.raises(Exception) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the exception message contains "NoSuchKey"
        assert "No files Found" in str(excinfo.value)

    def test_lambda_raises_error_on_corrupted_csv(self, s3_client):
        # Setup
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

        # Seed: Upload a corrupted CSV file with binearis/incorrect content
        corrupted_content = b"\xff\xfe\xfd\x12"
        file_name = "new_data/corrupted.csv"

        s3_client.put_object(
            Bucket=source_bucket,
            Key=file_name,
            Body=corrupted_content,
        )

        mock_event = {
            "file_to_obfuscate": "s3://source-bucket-test/new_data/corrupted.csv",
            "pii_fields": ["name", "email_address"],
        }

        # Assert: Expect an exception due to incorrect file content
        with pytest.raises(Exception) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the exception message contains
        assert "codec can't decode byte" in str(excinfo.value)

    def test_lambda_obfuscator_raises_error_for_empty_input_data(self, s3_client):
        # 1. Prepare empty test data
        headers = [
            "student_id",
            "name",
            "course",
            "cohort",
            "graduation_date",
            "email_address",
        ]
        test_input = []

        # Setup
        file_name = "empty_data.csv"
        ingest_bucket = "source-bucket-test"
        dest_bucket = "dest-bucket-test"

        s3_client.create_bucket(
            Bucket=ingest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input, columns=headers)
        wr.s3.to_csv(
            df=df_input, path=f"s3://{ingest_bucket}/new_data/{file_name}", index=False
        )

        mock_event = {
            "file_to_obfuscate": "s3://source-bucket-test/new_data/empty_data.csv",
            "pii_fields": ["name", "email_address"],
        }

        # Assert: Expect an exception due to empty input data
        with pytest.raises(ValueError) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the ValueError message contains
        assert (
            f"Error {mock_event["file_to_obfuscate"]}: The input data is empty."
            in str(excinfo.value)
        )

    def test_lambda_obfuscator_raise_error_no_primary_key_detected(self, s3_client):
        # 1. Prepare test data without primary key
        headers = ["nin", "name", "course", "email_address", "graduation_date"]
        test_input = [
            [
                "QQ123456B",
                "John Smith",
                "Software",
                "j.smith@email.com",
                "2024-03-31",
            ],
            [
                "QK623758C",
                "Julien Smith",
                "DevOps",
                "j.smith@email.com",
                "2024-03-31",
            ],
            [
                "PP225451B",
                "Jane Doe",
                "Data Science",
                "j.doe@email.com",
                "2024-01-15",
            ],
        ]
        # Setup
        file_name = "no_primary_key.csv"
        ingest_bucket = "ingestion-bucket-test"
        dest_bucket = "dest-bucket-test"

        s3_client.create_bucket(
            Bucket=ingest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input, columns=headers)
        wr.s3.to_csv(
            df=df_input,
            path=f"s3://ingestion-bucket-test/new_data/{file_name}",
            index=False,
        )

        mock_event = {
            "file_to_obfuscate": "s3://ingestion-bucket-test/new_data/no_primary_key.csv",
            "pii_fields": ["nin", "name", "email_address"],
        }

        # Assert: Expect an exception due to no primary key detected
        with pytest.raises(ValueError) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the ValueError message contains
        assert (
            f"No primary key detected in {mock_event["file_to_obfuscate"]}."
            f"Data records must be supplied with primary key."
            in str(excinfo.value)
        )

    def test_lambda_obfuscator_if_primary_key_given_csv_file(self, s3_client):
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
                "1S34",
                "John Smith",
                "Software",
                "2024-03-31",
                "2024-03-31",
                "j.smith@email.com",
            ],
            [
                "1S35",
                "John Smith",
                "Software",
                "2024-03-31",
                "2024-03-31",
                "j.smith@email.com",
            ],
            [
                "3D78",
                "Jane Doe",
                "Data Science",
                "2024-01-15",
                "2024-01-15",
                "j.doe@email.com",
            ],
        ]
        expected_output = [
            ["1S34", "***", "Software", "2024-03-31", "2024-03-31", "***"],
            ["1S35", "***", "Software", "2024-03-31", "2024-03-31", "***"],
            ["3D78", "***", "Data Science", "2024-01-15", "2024-01-15", "***"],
        ]

        # 2. SETUP Mock Buckets and Env Variables
        file_name = "test_data.csv"
        ingest_bucket = "gdpr-ingestion-bucket-test"
        dest_bucket = "gdpr-obfuscated-bucket-test"

        s3_client.create_bucket(
            Bucket=ingest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input, columns=headers)
        wr.s3.to_csv(
            df=df_input, path=f"s3://{ingest_bucket}/new_data/{file_name}", index=False
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "file_to_obfuscate": "s3://gdpr-ingestion-bucket-test/new_data/test_data.csv",
            "pii_fields": ["name", "email_address"],
            "primary_key": "student_id",
        }

        lambda_handler(mock_event, None)

        # 4. ASSERT: Read the result from the dest_bucket
        result_df = wr.s3.read_csv(
            f"s3://{dest_bucket}/obfuscated/new_data/test_data.csv"
        )

        expected_df = pd.DataFrame(expected_output, columns=headers)
        # Compare result with the expected output dataframe
        pdt.assert_frame_equal(result_df, expected_df)
        # Additional specific assertions
        assert result_df["name"][1] == "***"
        assert result_df["email_address"][2] == "***"
        assert result_df["student_id"][0] == "1S34"
        assert result_df["student_id"][1] == "1S35"
        assert result_df["student_id"][2] == "3D78"
        assert result_df["course"][2] == "Data Science"
        assert result_df["graduation_date"][2] == "2024-01-15"
        assert result_df.shape[0] == 3

    def test_lambda_obfuscator_raises_error_no_pii_fields_obfuscated(self, s3_client):
        # 1. Prepare test data with no PII fields
        headers = ["student_id", "course", "cohort", "graduation_date"]
        test_input = [
            [1234, "Software", "2024-03-31", "2024-03-31"],
            [5678, "Data Science", "2024-01-15", "2024-01-15"],
        ]

        # Setup
        file_name = "no_pii_fields.csv"
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

        # Create input DataFrame and upload to Mock S3
        df_input = pd.DataFrame(test_input, columns=headers)
        wr.s3.to_csv(
            df=df_input, path=f"s3://{source_bucket}/new_data/{file_name}", index=False
        )

        s3_client.put_object(
            Bucket=source_bucket,
            Key=file_name,
            Body=df_input.to_csv(index=False).encode("utf-8"),
        )

        mock_event = {
            "file_to_obfuscate": "s3://source-bucket-test/new_data/no_pii_fields.csv",
            "pii_fields": ["name", "email_address"],
        }

        # Assert: Expect an exception due to no PII fields found
        with pytest.raises(Exception) as excinfo:
            lambda_handler(mock_event, None)

        # Opcional: Check the exception message contains
        assert "No PII columns found to obfuscate." in str(excinfo.value)

    def test_obfuscate_data_unsupported_format_raises_error(self, s3_client):
        # Setup S3 buckets and env variables
        source_bucket = "source-bucket-test"
        dest_bucket = "destination-bucket-test"

        s3_client.create_bucket(
            Bucket=source_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        s3_client.create_bucket(
            Bucket=dest_bucket,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        os.environ["DESTINATION_BUCKET"] = dest_bucket

        # SEED: UpLoad an UNSUPPORTED file from your local folder to S3
        file_name = "dummy.txt"

        s3_client.put_object(
            Bucket=source_bucket,
            Key=file_name,
            Body="This is a txt file, not supported format.",
        )

        # 3. ACT: Trigger the handler with EventBridge style event
        mock_event = {
            "file_to_obfuscate": "s3://source-bucket-test/new_data/dummy.txt",
            "pii_fields": ["name", "email_address"],
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
