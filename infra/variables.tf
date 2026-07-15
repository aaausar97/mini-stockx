variable "aws_region" {
  description = "AWS region for SNS and SQS."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Prefix for resource names (e.g. stockx → stockx-order-matched)."
  type        = string
  default     = "stockx"
}
