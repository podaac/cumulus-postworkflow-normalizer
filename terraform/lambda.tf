resource "aws_lambda_function" "postworkflow_normalizer" {
  filename      = "${path.module}/postworkflow-normalizer.zip"
  function_name = "${var.prefix}-postworkflow-normalizer"
  source_code_hash = filebase64sha256("${path.module}/postworkflow-normalizer.zip")
  handler       = "cumulus_postworkflow_normalizer.lambda_handler.handler"
  role          = var.lambda_role
  runtime       = "python3.11"
  timeout       = var.timeout
  memory_size   = var.memory_size

  architectures = var.architectures

  environment {
    variables = {
      LOGGING_LEVEL               = var.log_level
    }
  }

  vpc_config {
    subnet_ids         = var.subnet_ids
    security_group_ids = var.security_group_ids
  }

  tags = local.tags
}
