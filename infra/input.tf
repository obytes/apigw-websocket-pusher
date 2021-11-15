######################
#     VARIABLES      |
######################
variable "prefix" {}

variable "common_tags" {
  type = map(string)
}

variable "connections_table" {
  type = object({
    name = string
    arn  = string
  })
}

variable "agma_arn" {}

variable "apigw_endpoint" {}

variable "notifications_topic_arn" {}

variable "notifications_queue_arn" {}
