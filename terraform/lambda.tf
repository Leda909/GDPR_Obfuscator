# Zip Lambda layer: obfuscator_lib.py and __init__.py
data "archive_file" "obfuscator_layer_zip" {
  type        = "zip"
  output_path = "${path.module}/../deployment/obfuscator_layer.zip"

  # Set up obfuscator_lib.py in the correct folder structure for Lambda layers 
  source {
    content  = file("${path.module}/../src/utils/obfuscator_lib.py")
    filename = "python/utils/obfuscator_lib.py"
  }

  source {
    content  = file("${path.module}/../src/utils/__init__.py")
    filename = "python/utils/__init__.py"
  }
}

# Zip Lambda function: lambda_function.py
data "archive_file" "obfuscator_lambda_zip" {
  type        = "zip"
  source_file = "${path.module}/../src/lambda_function.py"
  output_path = "${path.module}/../deployment/obfuscator_lambda.zip"
}

# Create the Lambda layer for obfuscator library
resource "aws_lambda_layer_version" "obfuscator_lib_layer" {
  filename            = data.archive_file.obfuscator_layer_zip.output_path
  layer_name          = "obfuscator_library_layer"
  compatible_runtimes = ["python3.12"]
  source_code_hash    = data.archive_file.obfuscator_layer_zip.output_base64sha256
}

# Create the obfuscator lambda function with two layers
resource "aws_lambda_function" "gdpr_obfuscator_lambda" {
  filename      = data.archive_file.obfuscator_lambda_zip.output_path
  function_name = "lambda_calling_obfuscator_lib"
  role          = aws_iam_role.lambda_role.arn
  handler       = "lambda_function.lambda_handler" # <-- file_name.function_name
  runtime       = "python3.12"
  # set memory size and timeout
  memory_size = 512 # <-- it can go down to 256MB if needed
  timeout     = 60

  # TWO LAYERS: awswrangler|pandas + obfuscator library
  layers = [
    "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python312:17",
    aws_lambda_layer_version.obfuscator_lib_layer.arn
  ]
  # compute the source code hash for the lambda function
  source_code_hash = data.archive_file.obfuscator_lambda_zip.output_base64sha256

  environment {
    variables = {
      DESTINATION_BUCKET = aws_s3_bucket.obfuscated_bucket.bucket
      PII_FIELDS = join(",", var.pii_fields)
    }
  }
}