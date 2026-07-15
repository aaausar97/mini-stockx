output "aws_region" {
  description = "Region where resources were created."
  value       = var.aws_region
}

output "aws_account_id" {
  description = "AWS account that owns these resources."
  value       = data.aws_caller_identity.current.account_id
}

output "sns_topic_arn" {
  description = "Paste into .env as SNS_TOPIC_ARN."
  value       = aws_sns_topic.order_matched.arn
}

output "sqs_payment_url" {
  description = "Paste into .env as SQS_PAYMENT_URL."
  value       = aws_sqs_queue.payment.url
}

output "sqs_notify_url" {
  description = "Paste into .env as SQS_NOTIFY_URL."
  value       = aws_sqs_queue.notify.url
}

output "msk_enabled" {
  description = "Whether MSK was provisioned."
  value       = var.enable_msk
}

output "kafka_bootstrap_servers" {
  description = "Public SASL/SCRAM bootstrap string for .env KAFKA_BOOTSTRAP_SERVERS."
  value       = var.enable_msk ? data.aws_msk_bootstrap_brokers.main[0].bootstrap_brokers_public_sasl_scram : null
}

output "kafka_username" {
  description = "SCRAM username for .env KAFKA_USERNAME."
  value       = var.enable_msk ? var.kafka_username : null
}

output "kafka_password" {
  description = "SCRAM password for .env KAFKA_PASSWORD."
  value       = var.enable_msk ? random_password.kafka[0].result : null
  sensitive   = true
}

output "msk_cluster_arn" {
  description = "MSK cluster ARN."
  value       = var.enable_msk ? aws_msk_cluster.main[0].arn : null
}
