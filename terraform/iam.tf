# ------------------------------
# Lambda IAM Role & Trust Policy
# ------------------------------

# Define role: allows lambda to assume this role
data "aws_iam_policy_document" "trust_policy" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Create the IAM Role for Lambda
resource "aws_iam_role" "lambda_role" {
  name_prefix        = "gdpr_obfuscator_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.trust_policy.json
}

# ------------------------------
# Lambda IAM Policy to S3 and CloudWatch Logs
# ------------------------------

# Define the Combined: S3 and CloudWatch policy document
data "aws_iam_policy_document" "lambda_combined_S3_cloudewatch_policy_doc" {
  # Define S3 policy to allow lambda to read/write to S3 buckets
  statement {
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket"
    ]
    resources = [
      "${aws_s3_bucket.ingestion_bucket.arn}",
      "${aws_s3_bucket.obfuscated_bucket.arn}",
      "${aws_s3_bucket.ingestion_bucket.arn}/*",
      "${aws_s3_bucket.obfuscated_bucket.arn}/*"
    ]
  }

  # Define CloudWatch Logs policy to allow lambda to write logs and metrics
  statement {
    effect = "Allow"
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:GetLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
      "logs:FilterLogEvents",
      "logs:StartQuery",
      "logs:GetQueryResults",
      "cloudwatch:PutMetricData",
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DescribeAlarms",
      "cloudwatch:DeleteAlarms"
    ]
    resources = ["arn:aws:logs:*:*:*"]
  }
}

# Create the Combined: S3 and CloudeWatch policy
resource "aws_iam_policy" "lambda_s3_coudewatch_policy" {
  name_prefix = "lambda-s3-cloudewatch-policy-"
  policy      = data.aws_iam_policy_document.lambda_combined_S3_cloudewatch_policy_doc.json
}

# Attach the Combined: S3 and CloudWatch policy to the Lambda role
resource "aws_iam_role_policy_attachment" "lambda_s3_write_policy_attachment" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = aws_iam_policy.lambda_s3_coudewatch_policy.arn
}

#------------------------------
# Enable the source bucket to send events to EventBridge
# ------------------------------

resource "aws_s3_bucket_notification" "bucket_eventbridge_signal" {
  bucket      = aws_s3_bucket.ingestion_bucket.id
  eventbridge = true
}

# ------------------------------
# EventBridge Rule: Trigger Lambda on S3 Object Created Events
# ------------------------------

# Define the pattern: Trigger when an object is created in the ingestion bucket
resource "aws_cloudwatch_event_rule" "s3_object_upload_rule" {
  name_prefix   = "s3-object-uploaded-rule-"
  description   = "Trigger Lambda on S3 Object Created Events"
  event_pattern = <<PATTERN
{
  "source": ["aws.s3"],
  "detail-type": ["Object Created"],
  "detail": {
    "bucket": { "name": ["${aws_s3_bucket.ingestion_bucket.id}"]},
    "object": { 
      "key": [ 
        { "suffix": ".csv" },
        { "suffix": ".json" },
        { "suffix": ".parquet" } 
      ]
    }
  }
}
PATTERN
}

# Set the Lambda as the target for the rule
resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.s3_object_upload_rule.name
  target_id = "TriggerObfuscator"
  arn       = aws_lambda_function.gdpr_obfuscator_lambda.arn
}

# ------------------------------
# Lambda Permission: Allow for EventBridge to invoke Lambda
# ------------------------------
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.gdpr_obfuscator_lambda.function_name
  principal     = "events.amazonaws.com"

  # specify the source ARN to limit permission to specific bucket
  source_arn = aws_cloudwatch_event_rule.s3_object_upload_rule.arn
}