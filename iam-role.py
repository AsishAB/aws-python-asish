import json, boto3
from dotenv import load_dotenv
import os
load_dotenv()

aws_access_key_id = os.getenv('AWS_ACCESS_KEY')
aws_secret_access_key = os.getenv('AWS_SECRET_KEY')
aws_region_name = os.getenv('AWS_REGION_NAME')
aws_input_s3_bucket = os.getenv('AWS_S3_SOURCE_BUCKET')
aws_account_id = os.getenv('AWS_ACCOUNT_ID')
aws_cloudwatch_log_group = os.getenv("AWS_CLOUDWATCH_LOG_GROUP")

role_name = "AWSRoleWithPython-ASISH"
policy_name = "policy-for-s3-redshift-using-python-ASISH"
policy_arn = f"arn:aws:iam::{aws_account_id}:policy/{policy_name}"


iam = boto3.client("iam",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region_name
    )

def create_iam_role():
    

    assume_role_policy_document = json.dumps({
        "Version": "2012-10-17",
        "Statement": [
            {
            "Effect": "Allow",
            "Principal": {
                "Service": [
                    "glue.amazonaws.com",
                    "s3.amazonaws.com",
                    "redshift.amazonaws.com",
                    "cloudwatch.amazonaws.com"
                    
                ]
            },
            "Action": "sts:AssumeRole"
            }
        ]
    })

    response = iam.create_role(
        RoleName = role_name,
        AssumeRolePolicyDocument = assume_role_policy_document
    )

    return response["Role"]["RoleName"]


def create_iam_policy():
 
    # Create a policy
    my_managed_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "glue:*",
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListAllMyBuckets",
                    "s3:GetBucketAcl",
                    "s3:ListBucketVersions",
                    "iam:ListRolePolicies",
                    "iam:GetRole",
                    "iam:ListRoles",
                    "iam:GetRolePolicy",
                    "cloudwatch:PutMetricData"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:CreateBucket"
                ],
                "Resource": [
                    f"arn:aws:s3:::{aws_input_s3_bucket}"
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    f"arn:aws:s3:::{aws_input_s3_bucket}",
                    f"arn:aws:s3:::{aws_input_s3_bucket}/*",
                ]
            },
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": [
                    f"arn:aws:logs:*:*:*:/{aws_cloudwatch_log_group}/*"
                ]
            },
        ]
    }
    response = iam.create_policy(
        PolicyName=policy_name,
        PolicyDocument=json.dumps(my_managed_policy)
    )
    # print(response)


def attach_iam_policy(policy_arn, role_name):
    response = iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=policy_arn
    )

    # print(response)


create_iam_policy()
python_create_role = create_iam_role()
attach_iam_policy(policy_arn, role_name)