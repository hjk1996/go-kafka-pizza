provider "aws" {
  region  = "ap-northeast-2"
}

resource "aws_dynamodb_table" "pizza_table" {
    name = "pizza"
    hash_key = "id"
    range_key = "createdAt"
    billing_mode = "PAY_PER_REQUEST"


    attribute {
      name = "id"
     type = "S"
    }

    attribute {
      name = "createdAt"
      type = "S"
    }

}