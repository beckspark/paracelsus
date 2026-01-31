# EventBridge Scheduler for ELT Pipeline Orchestration
# Simpler pattern than Step Functions, works reliably with LocalStack

# IAM role for EventBridge to invoke Lambda
resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project}-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
      }
    ]
  })

  tags = local.common_tags
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "${var.project}-eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "lambda:InvokeFunction"
        Resource = aws_lambda_function.trigger_meltano.arn
      }
    ]
  })
}

# EventBridge rule for scheduled ELT execution
# Triggers daily at 6 AM UTC
resource "aws_cloudwatch_event_rule" "daily_elt" {
  name                = "${var.project}-daily-elt"
  description         = "Trigger unified ELT pipeline daily at 6 AM UTC"
  schedule_expression = "cron(0 6 * * ? *)"
  state               = "ENABLED" # Enabled for ECS orchestration demo

  tags = local.common_tags
}

# EventBridge target - invoke Lambda function
resource "aws_cloudwatch_event_target" "trigger_meltano" {
  rule      = aws_cloudwatch_event_rule.daily_elt.name
  target_id = "trigger-meltano-elt"
  arn       = aws_lambda_function.trigger_meltano.arn
  role_arn  = aws_iam_role.eventbridge_role.arn

  input = jsonencode({
    job          = "elt-all"
    environment  = "dev"
    triggered_by = "scheduled"
  })
}

# Permission for EventBridge to invoke Lambda
resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridgeInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.trigger_meltano.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_elt.arn
}
