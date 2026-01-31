terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

# Configure AWS provider to use LocalStack
provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "us-east-1"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3             = "http://localstack:4566"
    lambda         = "http://localstack:4566"
    stepfunctions  = "http://localstack:4566"
    eventbridge    = "http://localstack:4566"
    cloudwatchlogs = "http://localstack:4566"
    iam            = "http://localstack:4566"
  }

  # LocalStack requires s3_use_path_style for bucket operations
  s3_use_path_style = true
}

# Variables
variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "paracelsus"
}

# Common tags
locals {
  common_tags = {
    Project     = var.project
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

# Outputs
output "s3_landing_bucket" {
  value = local.landing_bucket_id
}

output "lambda_trigger_meltano_arn" {
  value = aws_lambda_function.trigger_meltano.arn
}

output "eventbridge_rule_arn" {
  value = aws_cloudwatch_event_rule.daily_elt.arn
}
