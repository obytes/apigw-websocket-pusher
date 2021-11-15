resource "aws_sns_topic_subscription" "_" {
  topic_arn = var.notifications_topic_arn
  protocol  = "lambda"
  endpoint  = module.pusher.lambda["alias_arn"]
}

resource "aws_lambda_permission" "_" {
  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = module.pusher.lambda["arn"]
  qualifier     = module.pusher.lambda["alias"]
  principal     = "sns.amazonaws.com"
  source_arn    = var.notifications_topic_arn
}
