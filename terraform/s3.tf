# S3 Buckets for data landing zone
# Note: The landing bucket is created by the seeder service
# We use a data source to reference it rather than creating it

# Use data source to reference the bucket created by seeder
data "aws_s3_bucket" "landing" {
  bucket = "${var.project}-landing"
}

# Bucket for processed/archived data (Terraform manages this one)
resource "aws_s3_bucket" "processed" {
  bucket = "${var.project}-processed"

  tags = merge(local.common_tags, {
    Name = "${var.project}-processed"
  })
}

# For Terraform references, use the data source
locals {
  landing_bucket_id  = data.aws_s3_bucket.landing.id
  landing_bucket_arn = data.aws_s3_bucket.landing.arn
}
