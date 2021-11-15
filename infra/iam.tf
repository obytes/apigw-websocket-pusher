data "aws_iam_policy_document" "custom_policy_doc" {
  statement {
    actions = [
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:DeleteItem",
    ]

    resources = [
      var.connections_table.arn,
    ]
  }

  statement {
    actions = [
      "execute-api:ManageConnections",
    ]

    resources = [
      "${var.agma_arn}/*"
    ]
  }

  statement {
    actions = [
      "sqs:ChangeMessageVisibility",
      "sqs:ChangeMessageVisibilityBatch",
      "sqs:DeleteMessage",
      "sqs:DeleteMessageBatch",
      "sqs:GetQueueAttributes",
      "sqs:ReceiveMessage"
    ]

    resources = [
      var.notifications_queue_arn
    ]
  }
}
