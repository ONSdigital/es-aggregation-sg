import json
import unittest
import unittest.mock as mock

import pandas as pd
from pandas.util.testing import assert_frame_equal

import aggregation_county_method

class mock_context():
    aws_request_id = 66

context_object = mock_context()

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
            json_content = json.loads(file.read())
            output = aggregation_county_method.lambda_handler(
                json_content,
                context_object
            )

            expected_df = (
                pd.read_json("tests/fixtures/agg_county_output.json")
            )

            response_df = (
                pd.read_json(output)
            )

            response_df = response_df.round(5)
            expected_df = expected_df.round(5)

            assert_frame_equal(response_df, expected_df)

    def test_method_general_exception(self):
        with open("tests/fixtures/imp_output_test.json", "r") as file:
            json_content = json.loads(file.read())
            with mock.patch("aggregation_county_method.pd.DataFrame") as mocked:
                mocked.side_effect = Exception("General exception")
                response = aggregation_county_method.lambda_handler(
                    json_content,
                    context_object
                )

                assert "success" in response
                assert response["success"] is False
                assert """General exception""" in response["error"]

    def test_method_key_error(self):
        # pass none value to trigger key index error
        response = aggregation_county_method.lambda_handler(
            None,
            context_object
        )
        assert """Key Error""" in response["error"]
