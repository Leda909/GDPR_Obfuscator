# Source bucket that need to be anonymized
resource "aws_s3_bucket" "ingestion_bucket" {
  bucket = "gdpr-ingestion-bucket-${random_id.id.hex}"
  force_destroy = true  # <-- allow to delete non empty at destroy
  tags = {
    Name = "GDPR Source S3 Data Bucket"
  }
}

# Destination bucket for anonymized data - Not necessery - For testing purpose
resource "aws_s3_bucket" "obfuscated_bucket" {
  bucket = "gdpr-obfuscated-bucket-${random_id.id.hex}"
  force_destroy = true  # <-- allow to delete non empty at destroy
  tags = {
    Name = "GDPR Obfuscated S3 Data Bucket"
  }
}

# Generate random id for bucket names to ensure uniqueness
resource "random_id" "id" {
  byte_length = 4
}