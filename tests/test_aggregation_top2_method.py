import json
import unittest

import mock

import aggregation_top2_method


class MockContext():
    aws_request_id = 66


context_object = MockContext()


class TestAggregationTop2Method(unittest.TestCase):

    def test_method_happy_path(self):
        """
        Tests a correct run produces the correct output for a known input.
        """
        # Load input file and run it through the function
        with open("tests/fixtures/top2_wrangler_input.json") as file:
            input_data = json.load(file)

        returned_value = aggregation_top2_method.lambda_handler(input_data, None)

        # Write the output to file
        file = open('tests/fixtures/top2_produced_method_output', 'w')
        file.write(returned_value)
        file.close()

        with open("tests/fixtures/top2_method_output.json") as file:
            method_output_comparison = json.load(file)

        with open("tests/fixtures/top2_produced_method_output") as file:
            produced_method_output = json.load(file)

        self.assertEqual(produced_method_output, method_output_comparison)

    @mock.patch('aggregation_top2_method.calc_top_two')
    def test_general_exception(self, mock_calc_top_two):
        """
        Tests any exceptions are captured correctly in a general exception.
        """
        with open("tests/fixtures/top2_wrangler_input.json") as file:
            input_data = json.load(file)

        mock_calc_top_two.side_effect = Exception("Whoops")

        returned_value = aggregation_top2_method.lambda_handler(input_data,
                                                                context_object)

        assert("""processing the method""" in returned_value['error'])
