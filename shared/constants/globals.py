DEV_SQS_DOWNSTREAM_QUEUE_URL = (
    "https://sqs.us-east-1.amazonaws.com/077756283408/dev-downstream-visionflo-agents"
)
DEV_SQS_DOWNSTREAM_QUEUE_NAME = "dev-downstream-visionflo-agents"
DEV_SQS_UPSTREAM_QUEUE_URL = (
    "https://sqs.us-east-1.amazonaws.com/077756283408/dev-upstream-visionflo-agents"
)
DEV_SQS_UPSTREAM_QUEUE_NAME = "dev-upstream-visionflo-agents"

PROD_SQS_DOWNSTREAM_QUEUE_URL = ""
PROD_SQS_UPSTREAM_QUEUE_URL = ""
PROD_SQS_DOWNSTREAM_QUEUE_NAME = ""
PROD_SQS_UPSTREAM_QUEUE_NAME = ""

SQS_VISIBILITY_TIMEOUT: int = 300
SQS_WAIT_TIME_SECONDS: int = 20
SQS_MAX_MESSAGES: int = 10
AWS_REGION = "us-east-1"

CELERY_POLLING_INTERVAL: int = 1
CELERY_VISIBILITY_EXTEND_SECONDS: int = 240
