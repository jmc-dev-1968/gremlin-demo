
import os

USE_SSH_TUNNEL = True

# get environment from system variable
ENV = os.getenv('NEPTUNE_ENV', 'DEV')
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY', '')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', '')
NEPTUNE_URL = os.getenv('NEPTUNE_URL', '')

if ENV == "DEV":

    USE_SSH_TUNNEL = True

    # neptune
    #NEPTUNE_URL = "xxxxxx.cluster-yyyyyyyyyyyy.ap-southeast-1.neptune.amazonaws.com" # can hard code is OK, remove from env vars if so
    NEPTUNE_PORT = "8182"

    # misc
    LOCAL_PORT = "8182"
    LOCAL_DATA_DIR = r".\data"

    # aws
    AWS_REGION = "ap-southeast-1"
    IAM_ROLE_ARN = "arn:aws:iam::816603124080:role/Neptune-S3-Loader"

    #s3
    S3_BUCKET = "jeffery-m-cooper-external"
    S3_PREFIX = "neptune"


elif ENV == "PROD":

    pass


