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

variable "enable_msk" {
  description = "Provision Amazon MSK instead of using local docker-compose Kafka."
  type        = bool
  default     = false
}

variable "kafka_version" {
  description = "Kafka version for MSK."
  type        = string
  default     = "3.6.0"
}

variable "kafka_username" {
  description = "SCRAM username written to Secrets Manager."
  type        = string
  default     = "stockx"
}

variable "msk_broker_count" {
  description = "MSK broker nodes (minimum 2)."
  type        = number
  default     = 2
}

variable "msk_instance_type" {
  description = "MSK broker instance type."
  type        = string
  default     = "kafka.t3.small"
}
