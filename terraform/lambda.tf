# Lambda Function for Unified ELT Pipeline Orchestration
# Triggers Meltano jobs which include both EL and dbt Transform

# IAM role for Lambda functions
resource "aws_iam_role" "lambda_role" {
  name = "${var.project}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

# IAM policy for Lambda
resource "aws_iam_role_policy" "lambda_policy" {
  name = "${var.project}-lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          local.landing_bucket_arn,
          "${local.landing_bucket_arn}/*"
        ]
      }
    ]
  })
}

# Lambda: Trigger Meltano ELT Jobs (unified EL + Transform)
data "archive_file" "trigger_meltano" {
  type        = "zip"
  source_file = "${path.module}/lambda/trigger_meltano.py"
  output_path = "${path.module}/lambda/trigger_meltano.zip"
}

resource "aws_lambda_function" "trigger_meltano" {
  filename         = data.archive_file.trigger_meltano.output_path
  function_name    = "${var.project}-trigger-meltano"
  role             = aws_iam_role.lambda_role.arn
  handler          = "trigger_meltano.handler"
  source_code_hash = data.archive_file.trigger_meltano.output_base64sha256
  runtime          = "python3.11"
  timeout          = 900 # 15 minutes for unified ELT

  environment {
    variables = {
      MELTANO_ENVIRONMENT = var.environment
    }
  }

  tags = local.common_tags
}

# CloudWatch Log Group
resource "aws_cloudwatch_log_group" "trigger_meltano" {
  name              = "/aws/lambda/${aws_lambda_function.trigger_meltano.function_name}"
  retention_in_days = 7

  tags = local.common_tags
}
