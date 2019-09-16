import os

import boto3
import pytest
from moto import mock_lambda, mock_s3, mock_sns, mock_sqs


@pytest.fixture(scope='session')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='session')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')


@pytest.fixture(scope='session')
def sqs(aws_credentials):
    with mock_sqs():
        yield boto3.client('sqs', region_name='eu-west-2')


@pytest.fixture(scope='session')
def sns(aws_credentials):
    with mock_sns():
        yield boto3.client('sns', region_name='eu-west-2')


@pytest.fixture(scope='session')
def awslambda(aws_credentials):
    with mock_lambda():
        yield boto3.client('lambda', region_name='eu-west-2')
=======
import os

import boto3
import pytest
from moto import mock_s3, mock_sns, mock_sqs


@pytest.fixture(scope='session')
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'


@pytest.fixture(scope='session')
def sns(aws_credentials):
    with mock_sns():
        yield boto3.client('sns', region_name='eu-west-2')


@pytest.fixture(scope='session')
def sqs(aws_credentials):
    with mock_sqs():
        yield boto3.client('sqs', region_name='eu-west-2')


@pytest.fixture(scope='session')
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client('s3', region_name='eu-west-2')
