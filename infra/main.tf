resource "aws_sns_topic" "order_matched" {
  name = "${var.project_name}-order-matched"

  tags = {
    Project = var.project_name
  }
}

resource "aws_sqs_queue" "payment" {
  name = "${var.project_name}-payment"

  tags = {
    Project = var.project_name
  }
}

resource "aws_sqs_queue" "notify" {
  name = "${var.project_name}-notify"

  tags = {
    Project = var.project_name
  }
}

resource "aws_sqs_queue_policy" "payment" {
  queue_url = aws_sqs_queue.payment.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.payment.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_sns_topic.order_matched.arn }
      }
    }]
  })
}

resource "aws_sqs_queue_policy" "notify" {
  queue_url = aws_sqs_queue.notify.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "sns.amazonaws.com" }
      Action    = "sqs:SendMessage"
      Resource  = aws_sqs_queue.notify.arn
      Condition = {
        ArnEquals = { "aws:SourceArn" = aws_sns_topic.order_matched.arn }
      }
    }]
  })
}

resource "aws_sns_topic_subscription" "payment" {
  topic_arn = aws_sns_topic.order_matched.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.payment.arn

  depends_on = [aws_sqs_queue_policy.payment]
}

resource "aws_sns_topic_subscription" "notify" {
  topic_arn = aws_sns_topic.order_matched.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.notify.arn

  depends_on = [aws_sqs_queue_policy.notify]
}
