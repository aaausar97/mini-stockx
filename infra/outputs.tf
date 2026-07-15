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
