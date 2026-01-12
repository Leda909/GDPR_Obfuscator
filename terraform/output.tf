# to get the S3 source bucket name
output "ingestion_bucket_name" {
  value = aws_s3_bucket.ingestion_bucket.id
}

# to get the S3 destination bucket name
output "destination_bucket_name" {
  value = aws_s3_bucket.obfuscated_bucket.id
}

# to get the obfuscator lambda layer arn
output "lambda_layer_arn" {
  value = aws_lambda_layer_version.obfuscator_lib_layer.arn
}

# to get the obfuscator lambda function arn
output "lambda_function_arn" {
  value = aws_lambda_function.gdpr_obfuscator_lambda.arn
}