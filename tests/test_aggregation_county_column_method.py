import unittest
import unittest.mock as mock

import pandas as pd
from pandas.util.testing import assert_frame_equal

import aggregation_column_method


class MockContext():
    aws_request_id = 66


context_object = MockContext()


class TestCountyMethodMethods(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        cls.mock_os_patcher = mock.patch.dict(
            "os.environ",
            {
                "AWS_ACCESS_KEY_ID": "testing",
                "AWS_SECRET_ACCESS_KEY": "testing",
                "AWS_SECURITY_TOKEN": "testing",
                "AWS_SESSION_TOKEN": "testing"
            },
        )

        cls.mock_os_patcher.start()

    @classmethod
    def teardown_class(cls):
        cls.mock_os_patcher.stop()

    def test_method_happy_path(self):
        with open("tests/fixtures/imp_output_test.json", "r") as file:
            json_content = file.read()

            json_payload = {
                "input_json": json_content,
                "total_column": "Q608_total",
                "additional_aggregated_column": "region",
                "aggregated_column": "county",
                "cell_total_column": "county_total",
                "aggregation_type": "sum"
            }

            output = aggregation_column_method.lambda_handler(
                json_payload,
                context_object
            )

            expected_df = (
                pd.read_json("tests/fixtures/agg_county_output.json")
            )

            response_df = (
                pd.read_json(output["data"])
            )

            response_df = response_df.round(5)
            expected_df = expected_df.round(5)

            assert_frame_equal(response_df, expected_df)

    def test_method_general_exception(self):
        with open("tests/fixtures/imp_output_test.json", "r") as file:
            json_content = file.read()

            json_payload = {
                "input_json": json_content,
                "total_column": "Q608_total",
                "additional_aggregated_column": "region",
                "aggregated_column": "county",
                "cell_total_column": "county_total",
                "aggregation_type": "sum"
            }

            with mock.patch("aggregation_column_method.pd.DataFrame") as mocked:
                mocked.side_effect = Exception("General exception")
                response = aggregation_column_method.lambda_handler(
                    json_payload,
                    context_object
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    def test_method_key_error(self):
        # pass none value to trigger key index error
        with open("tests/fixtures/imp_output_test.json", "r") as file:
            json_content = file.read()

        json_payload = {
            "input_json": json_content,
            "total_column": "Q608_total",
            "additional_aggregated_column": "region",
            "aggregated_column": "county",
            "cell_total_column": "county_total",
            "aggregation_type": "sum"
        }

        with mock.patch("aggregation_column_method.json.loads") as mocked:
            mocked.side_effect = KeyError("Key Error")

            response = aggregation_column_method.lambda_handler(
                json_payload,
                context_object
            )

            assert """Key Error""" in response["error"]
