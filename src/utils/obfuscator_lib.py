import awswrangler as wr
import pandas as pd
from io import BytesIO
import logging

# Configure logger for this module
logger = logging.getLogger(__name__)


# ==========================================================
# OBFUSCATOR (LIBRARY MODULE)
# Independent tool that can be called from any procedure, returns a byte stream.
# ==========================================================
def obfuscate_data(s3_source_path, pii_fields, primary_key=None):
    """
    General purpose Obfuscator|Pseudonymizator tool, which returns a byte stream.
    MVP: CSV files, Extended: json, parquet

    Args:
        s3_source_path (str): S3 URI of source file (s3://source_bucket/new_data/test_data.csv)
        pii_fields (list): List of the column names to be obfuscated [***].
        primary_key (str, optional): Primary key column name. None for auto-detect.

    Returns:
        BytesIO: A byte stream object containing the obfuscated same data format.

    Raises:
        Exception: unsupported file formats
        ValueError: empty input data
        ValueError: no primary key detectable
        Exception: no PII columns found to obfuscate
        Exception: general errors during obfuscator execution
    """
    try:
        # Determine file extension s3_source_path = s3://bucket/folder/file.csv
        extension = s3_source_path.split(".")[
            -1
        ].lower()  # <-- 'csv', 'json', 'parquet'

        # 1. Load data based on format
        if extension == "csv":
            df = wr.s3.read_csv(s3_source_path)
        elif extension == "json":
            # important: orient="records" to match the json lines format
            df = wr.s3.read_json(s3_source_path, orient="records")
        elif extension == "parquet":
            df = wr.s3.read_parquet(s3_source_path)
        else:
            # This should not happen due to prior EventBridge validation, but for conirmation.
            logger.error(f"Unsupported format: {extension} from: {s3_source_path}")
            raise Exception(f"Unsupported format: {extension}")

        # Raise error for empty dataframe
        if df.empty:
            raise ValueError(f"Error {s3_source_path}: The input data is empty.")

        # ---- PRIMARY KEY VALIDATION ----
        # Logic: unique, no null, equal length in all rows, consistent type/pattern
        if not primary_key:
            pk_candidates = [
                col
                for col in df.columns
                if df[col].is_unique
                and not df[col].isnull().any()
                and df[col].astype(str).map(len).nunique() == 1
                # and not df[col].astype(str).str.contains(' ').any()
                and (
                    pd.api.types.is_string_dtype(df[col])
                    or pd.api.types.is_integer_dtype(df[col])
                )
            ]

            # remove pii fields from pk_candidates (eg NIN, phone_number, email)
            safe_pk_candidates = [col for col in pk_candidates if col not in pii_fields]

            if not safe_pk_candidates:
                logger.error(
                    f"No primary key detected in {s3_source_path}."
                    f"Data records must be supplied with primary key."
                )
                raise ValueError(
                    f"No primary key detected in {s3_source_path}."
                    f"Data records must be supplied with primary key."
                )

            # priority on df.comumns[0] if it is in pk_candidates
            primary_key = (
                df.columns[0]
                if df.columns[0] in safe_pk_candidates
                else safe_pk_candidates[0]
            )
        logger.info(f"primary_key: {primary_key}")

        # 2. --- OBFUSCATION ---
        # Ensure primary key is not obfuscated
        safe_pii_fields = [field for field in pii_fields if field != primary_key]

        logger.info(f"Starting Obfuscaton..., filtered_pii_fields: {safe_pii_fields}")

        count_obf_col = 0
        obf_pii_fields = []

        for col in safe_pii_fields:
            if col in df.columns:
                df[col] = "***"
                obf_pii_fields.append(col)
                count_obf_col += 1
                logger.info(f"obfuscated column: {col}")

        if count_obf_col == 0:
            logger.warning("No PII columns found to obfuscate.")
            raise Exception("No PII columns found to obfuscate.")

        logger.info(f"Successfully obfuscated {len(obf_pii_fields)} fields.")

        # 3. --- TRANSFORM back to BYTE STREAM ---
        # no formating, ('Exact Copy')
        output_buffer = BytesIO()

        if extension == "csv":
            df.to_csv(output_buffer, index=False)
        elif extension == "json":
            df.to_json(output_buffer, orient="records", lines=False, date_format="iso")
        elif extension == "parquet":
            df.to_parquet(output_buffer, index=False)

        logger.info(f"output_buffer: {output_buffer} created successfully.")

        # Reset buffer position to the beginning
        output_buffer.seek(0)

        return output_buffer

    # Error handling
    except Exception as e:
        logger.error(f"Error in obfuscate_data: {str(e)}")
        raise
