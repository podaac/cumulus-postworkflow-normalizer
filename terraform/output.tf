output "postworkflow_normalizer_arn" {
  value = aws_lambda_function.postworkflow_normalizer.arn
}

output "postworkflow_normalizer_name" {
  value = aws_lambda_function.postworkflow_normalizer.function_name
}