import json
import unittest

import aggregation_entref_method

class MockContext():
    aws_request_id = 66

context_object = MockContext()

class TestStringMethods(unittest.TestCase):

    def test_method_happy_path(self):
        with open("tests/fixtures/wrangler_input.json") as file:
            input_data = file.read()

            json_payload = {
                "input_json": input_data,
                "total_column": "Q608_total",
                "period_column": "period",
                "region_column": "region",
                "county_column": "county",
                "ent_ref_column": "enterprise_ref",
                "cell_total_column": "ent_ref_count"
            }

            returned_value = aggregation_entref_method.lambda_handler(json_payload, None)

            file = open('tests/fixtures/produced_method_output', 'w')
            file.write(json.dumps(returned_value))
            file.close()

            with open("tests/fixtures/method_output.json") as file:
                method_output_comparison = json.load(file)

            with open("tests/fixtures/produced_method_output") as file:
                produced_method_output = json.load(file)

            self.assertEqual(produced_method_output, method_output_comparison)

    def test_key_error_exception(self):

        with open("tests/fixtures/wrangler_input.json", "r") as file:
            content = file.read()
            content = content.replace("period", "TEST")
            json_content = content

            json_payload = {
                "input_json": json_content,
                "total_column": "Q608_total",
                "period_column": "period",
                "region_column": "region",
                "county_column": "county",
                "ent_ref_column": "enterprise_ref",
                "cell_total_column": "ent_ref_count"
            }

            returned_value = aggregation_entref_method.lambda_handler(
                json_payload, context_object)

            # If the method didn't produce an error it would mean the output is a string.
            self.assertIsNot(type(returned_value), str)
            assert "Key Error" in returned_value["error"]

    def test_general_exception(self):

        with open("tests/fixtures/wrangler_input.json") as file:
            input_data = json.load(file)

        returned_value = aggregation_entref_method.lambda_handler(
            str(input_data), context_object)

        assert "General Error" in returned_value["error"]
