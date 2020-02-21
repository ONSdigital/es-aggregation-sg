import json
import unittest
from unittest import mock

import boto3
import pandas as pd
from es_aws_functions import exception_classes
from moto import mock_s3, mock_sns, mock_sqs

import combiner  # noqa


class MockContext():
    aws_request_id = 66


context_object = MockContext()


class TestCombininator(unittest.TestCase):
    def test_missing_environment_variable(self):
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "sns_topic_arn": "not_an_arn"
            }
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                combiner.lambda_handler({"RuntimeVariables": {
                    "run_id": "bob",
                    "out_file_name": "mock_method",
                    "outgoing_message_group_id": "Bob",
                    "queue_url": "Earl"}},
                                        context_object)
            assert "Error validating environment" in exc_info.exception.error_message

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_happy_path(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "sns_topic_arn": "not_an_arn",
                "bucket_name": "mrsbucket"
            }
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with open("tests/fixtures/agg1.json") as file:
                agg1 = file.read()
            with open("tests/fixtures/agg2.json") as file:
                agg2 = file.read()
            with open("tests/fixtures/agg3.json") as file:
                agg3 = file.read()
            with mock.patch("combiner.aws_functions") as mock_funk:
                mock_funk.read_dataframe_from_s3.side_effect = \
                    [pd.DataFrame(json.loads(s3_data)), pd.DataFrame(json.loads(agg1)),
                     pd.DataFrame(json.loads(agg2)), pd.DataFrame(json.loads(agg3))]
                mock_funk.get_sqs_messages.return_value = {
                            "Messages": [
                                {"Body": "{\"key\": \"kee\","
                                         "\"bucket\":\"bouquet\"}",
                                 "ReceiptHandle": '666'},
                                {"Body": "{\"key\": \"kee\","
                                         "\"bucket\":\"bouquet\"}",
                                 "ReceiptHandle": '666'},
                                {"Body": "{\"key\": \"kee\","
                                         "\"bucket\":\"bouquet\"}",
                                 "ReceiptHandle": '666'}]
                        }
                out = combiner.lambda_handler(
                    {"RuntimeVariables": {"aggregated_column": "county",
                                          "additional_aggregated_column": "region",
                                          "run_id": "bob",
                                          "out_file_name": "mock_method",
                                          "outgoing_message_group_id": "Bob",
                                          "queue_url": sqs_queue_url,
                                          "in_file_name": "sss"
                                          }}, context_object)

                assert out["success"]

    @mock_sqs
    @mock_s3
    def test_no_data_in_queue(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "sns_topic_arn": "not_an_arn",
                "out_file_name": "mock_method",
                "bucket_name": "BertieBucket",
                "outgoing_message_group_id": "Bob"
            }
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with mock.patch("combiner.aws_functions.read_dataframe_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    combiner.lambda_handler(
                        {"RuntimeVariables": {"aggregated_column": "county",
                                              "additional_aggregated_column": "region",
                                              "run_id": "bob",
                                              "out_file_name": "mock_method",
                                              "outgoing_message_group_id": "Bob",
                                              "queue_url": sqs_queue_url,
                                              "in_file_name": "sss"
                                              }}, context_object)
                assert "There was no data in sqs queue" in \
                       exc_info.exception.error_message

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_attribute_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "sns_topic_arn": "not_an_arn",
                "out_file_name": "mock_method",
                "bucket_name": "BertieBucket",
                "outgoing_message_group_id": "Bob"
            }
        ):
            with mock.patch("combiner.aws_functions.read_dataframe_from_s3") as mock_bot:
                mock_bot.side_effect = AttributeError("noo")
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    combiner.lambda_handler(
                        {"RuntimeVariables": {"aggregated_column": "county",
                                              "additional_aggregated_column": "region",
                                              "run_id": "bob",
                                              "queue_url": sqs_queue_url,
                                              "out_file_name": "mock_method",
                                              "outgoing_message_group_id": "Bob",
                                              "in_file_name": "sss"
                                              }}, context_object)
                assert "Bad data encountered in" in exc_info.exception.error_message

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_client_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "sns_topic_arn": "not_an_arn",
                "out_file_name": "mock_method",
                "bucket_name": "BertieBucket",
                "outgoing_message_group_id": "Bob"
            }
        ):
            with unittest.TestCase.assertRaises(
                    self, exception_classes.LambdaFailure) as exc_info:
                combiner.lambda_handler(
                        {"RuntimeVariables": {"aggregated_column": "county",
                                              "additional_aggregated_column": "region",
                                              "run_id": "bob",
                                              "queue_url": sqs_queue_url,
                                              "out_file_name": "mock_method",
                                              "outgoing_message_group_id": "Bob",
                                              "in_file_name": "sss"
                                              }}, context_object)
            assert "AWS Error" in exc_info.exception.error_message

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_key_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "sns_topic_arn": "not_an_arn",
                "out_file_name": "mock_method",
                "bucket_name": "Bertie Bucket",
                "outgoing_message_group_id": "Bob"
            }
        ):
            with open("tests/fixtures/factorsdata.json") as file:
                s3_data = file.read()
            with open("tests/fixtures/agg1.json") as file:
                agg1 = file.read()
            with mock.patch("combiner.aws_functions.read_dataframe_from_s3") as mock_s3:
                mock_s3.return_value = s3_data
                with mock.patch("combiner.aws_functions.get_sqs_message") as mock_sqs:
                    with mock.patch("combiner.aws_functions.send_sns_message") as mock_sns:  # noqa

                        mock_sqs.return_value = {
                            "Messages": [
                                {"Boody": agg1},
                                {"Boody": agg1},
                                {"Boody": agg1},
                            ]
                        }
                        with unittest.TestCase.assertRaises(
                                self, exception_classes.LambdaFailure) as exc_info:
                            combiner.lambda_handler(
                                {"RuntimeVariables": {
                                    "aggregated_column": "county",
                                    "additional_aggregated_column": "region",
                                    "run_id": "bob",
                                    "queue_url": sqs_queue_url,
                                    "out_file_name": "mock_method",
                                    "outgoing_message_group_id": "Bob",
                                    "in_file_name": "sss"
                                    }}, context_object)
                        assert "Key Error" in exc_info.exception.error_message

    @mock_sqs
    @mock_s3
    @mock_sns
    def test_general_error(self):
        sqs = boto3.resource("sqs", region_name="eu-west-2")
        sqs.create_queue(QueueName="test_queue")
        sqs_queue_url = sqs.get_queue_by_name(QueueName="test_queue").url
        with mock.patch.dict(
            "os.environ",
            {
                "checkpoint": "mock_checkpoint",
                "sns_topic_arn": "not_an_arn",
                "out_file_name": "mock_method",
                "bucket_name": "BertieBucket",
                "outgoing_message_group_id": "Bob"
            }
        ):
            with mock.patch("combiner.aws_functions.read_dataframe_from_s3") as mock_bot:
                mock_bot.side_effect = Exception("noo")
                with unittest.TestCase.assertRaises(
                        self, exception_classes.LambdaFailure) as exc_info:
                    combiner.lambda_handler(
                        {"RuntimeVariables": {"aggregated_column": "county",
                                              "additional_aggregated_column": "region",
                                              "run_id": "bob",
                                              "queue_url": sqs_queue_url,
                                              "out_file_name": "mock_method",
                                              "outgoing_message_group_id": "Bob",
                                              "in_file_name": "sss"
                                              }}, context_object)
                assert "General Error" in exc_info.exception.error_message
