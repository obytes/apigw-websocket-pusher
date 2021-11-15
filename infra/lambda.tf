module "pusher" {
  source      = "git::https://github.com/obytes/terraform-aws-codeless-lambda.git//modules/lambda"
  prefix      = local.prefix
  common_tags = local.common_tags
  description = "API Gateway Websocket Asynchronous Notifications Pusher"

  runtime     = "python3.8"
  handler     = "app.main.handler"
  timeout     = 120
  memory_size = 1024
  policy_json = data.aws_iam_policy_document.custom_policy_doc.json

  envs    = {
    APIGW_ENDPOINT         = var.apigw_endpoint
    CONNECTIONS_TABLE_NAME = var.connections_table.name
  }
}
