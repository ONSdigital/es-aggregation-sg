import json
import unittest
import unittest.mock as mock

import boto3
import pandas as pd
from moto import mock_lambda, mock_s3, mock_sns

import aggregation_county_wrangler


class TestCountyWranglerMethods(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "bucket_name": "mock-bucket",
                "file_name": "mock-file",
                "queue_url": "mock-queue-url",
                "checkpoint": "mockpoint",
                "sns_topic_arn": "mock-topic-arn",
                "sqs_messageid_name": "mock-message-id",
                "method_name": "mock-method",
                "period": "mock-period"
            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        # Stop the mocking of the os stuff
        cls.mock_os_patcher.stop()

    def test_happy_path(self):
        