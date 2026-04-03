provider "aws" {
  region = "us-east-1"
}

############################
# RANDOM SUFFIX (UNIQUE NAMES)
############################
resource "random_id" "suffix" {
  byte_length = 4
}

############################
# S3 BUCKETS
############################
resource "aws_s3_bucket" "today" {
  bucket = "today-quest-${random_id.suffix.hex}"
}

resource "aws_s3_bucket" "yesterday" {
  bucket = "yesterday-quest-${random_id.suffix.hex}"
}

############################
# PUBLIC READ POLICIES
############################
data "aws_iam_policy_document" "today_public" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.today.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket_policy" "today_policy" {
  bucket = aws_s3_bucket.today.id
  policy = data.aws_iam_policy_document.today_public.json
}

data "aws_iam_policy_document" "yesterday_public" {
  statement {
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.yesterday.arn}/*"]

    principals {
      type        = "*"
      identifiers = ["*"]
    }
  }
}

resource "aws_s3_bucket_policy" "yesterday_policy" {
  bucket = aws_s3_bucket.yesterday.id
  policy = data.aws_iam_policy_document.yesterday_public.json
}

############################
# SQS QUEUE
############################
resource "aws_sqs_queue" "quest_queue" {
  name = "quest_sqs_queue"
}

############################
# ALLOW S3 → SQS
############################
data "aws_iam_policy_document" "sqs_policy" {
  statement {
    actions = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.quest_queue.arn]

    principals {
      type        = "Service"
      identifiers = ["s3.amazonaws.com"]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values = [
        aws_s3_bucket.today.arn,
        aws_s3_bucket.yesterday.arn
      ]
    }
  }
}

resource "aws_sqs_queue_policy" "allow_s3" {
  queue_url = aws_sqs_queue.quest_queue.id
  policy    = data.aws_iam_policy_document.sqs_policy.json
}

############################
# S3 → SQS NOTIFICATIONS
############################
resource "aws_s3_bucket_notification" "today_notify" {
  bucket = aws_s3_bucket.today.id

  queue {
    queue_arn     = aws_sqs_queue.quest_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"
  }

  depends_on = [aws_sqs_queue_policy.allow_s3]
}

resource "aws_s3_bucket_notification" "yesterday_notify" {
  bucket = aws_s3_bucket.yesterday.id

  queue {
    queue_arn     = aws_sqs_queue.quest_queue.arn
    events        = ["s3:ObjectCreated:*"]
    filter_suffix = ".json"
  }

  depends_on = [aws_sqs_queue_policy.allow_s3]
}

############################
# IAM ROLE - DAILY LAMBDA
############################
data "aws_iam_policy_document" "daily_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "daily_role" {
  name               = "daily_quest_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.daily_assume.json
}

data "aws_iam_policy_document" "daily_policy" {
  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject"
    ]
    resources = [
      aws_s3_bucket.today.arn,
      "${aws_s3_bucket.today.arn}/*",
      aws_s3_bucket.yesterday.arn,
      "${aws_s3_bucket.yesterday.arn}/*"
    ]
  }

  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "daily_attach" {
  role   = aws_iam_role.daily_role.id
  policy = data.aws_iam_policy_document.daily_policy.json
}

############################
# DAILY LAMBDA
############################
resource "aws_lambda_function" "daily_lambda" {
  function_name = "daily_quest_lambda"
  role          = aws_iam_role.daily_role.arn
  handler       = "index.lambda_handler"
  runtime       = "python3.11"

  filename         = "lambda.zip"
  source_code_hash = filebase64sha256("lambda.zip")

  timeout = 60

  environment {
    variables = {
      TODAY_BUCKET     = aws_s3_bucket.today.bucket
      YESTERDAY_BUCKET = aws_s3_bucket.yesterday.bucket
    }
  }
}

############################
# SCHEDULE 8AM (UTC)
############################
resource "aws_cloudwatch_event_rule" "daily_trigger" {
  name                = "daily-trigger"
  schedule_expression = "cron(0 8 * * ? *)"
}

resource "aws_cloudwatch_event_target" "daily_target" {
  rule = aws_cloudwatch_event_rule.daily_trigger.name
  arn  = aws_lambda_function.daily_lambda.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.daily_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.daily_trigger.arn
}

############################
# IAM ROLE - SQS LAMBDA
############################
data "aws_iam_policy_document" "sqs_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sqs_role" {
  name               = "process_sqs_lambda_role"
  assume_role_policy = data.aws_iam_policy_document.sqs_assume.json
}

data "aws_iam_policy_document" "sqs_lambda_policy" {
  statement {
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes"
    ]
    resources = [aws_sqs_queue.quest_queue.arn]
  }

  statement {
    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents"
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sqs_attach" {
  role   = aws_iam_role.sqs_role.id
  policy = data.aws_iam_policy_document.sqs_lambda_policy.json
}

############################
# SQS PROCESSING LAMBDA
############################
resource "aws_lambda_function" "sqs_lambda" {
  function_name = "process_sqs_queue_msgs_lambda"
  role          = aws_iam_role.sqs_role.arn
  handler       = "index.lambda_handler"
  runtime       = "python3.11"

  filename         = "lambda_sqs.zip"
  source_code_hash = filebase64sha256("lambda_sqs.zip")

  timeout = 30
}

############################
# SQS → LAMBDA TRIGGER
############################
resource "aws_lambda_event_source_mapping" "sqs_trigger" {
  event_source_arn = aws_sqs_queue.quest_queue.arn
  function_name    = aws_lambda_function.sqs_lambda.arn
  batch_size       = 5
}