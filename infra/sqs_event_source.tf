resource "aws_lambda_event_source_mapping" "_" {
  enabled                            = true
  batch_size                         = 10
  event_source_arn                   = var.notifications_queue_arn
  function_name                      = module.pusher.lambda["alias_arn"]
  maximum_batching_window_in_seconds = 0 # Do not wait until batch size is fulfilled
}
