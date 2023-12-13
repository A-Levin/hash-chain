import hashlib
import unittest
from unittest.mock import patch

from main import calculate_cell_hash, calculate_row_hash, calculate_table_hash


class TestMain(unittest.TestCase):
    def test_calculate_first_cell_hash(self):
        cell_data = "some_data"
        previous_cell_hash = ""

        result = calculate_cell_hash(cell_data, previous_cell_hash)
        expected_result = hashlib.sha256((cell_data + previous_cell_hash).encode("utf-8")).hexdigest()
        self.assertEqual(result, expected_result, "Ошибка в calculate_cell_hash")

    def test_calculate_second_cell_hash(self):
        cell_data = "some_data"
        previous_cell_hash = "some_previous_data"

        result = calculate_cell_hash(cell_data, previous_cell_hash)
        expected_result = hashlib.sha256((cell_data + previous_cell_hash).encode("utf-8")).hexdigest()
        self.assertEqual(result, expected_result, "Ошибка в calculate_cell_hash")

    def test_calculate_row_hash(self):
        row_data = ["some_data", "some_data_1", "some_data_2", "some_data_3"]
        result = calculate_row_hash(row_data)
        previous, expected_result = "", ""
        for row in row_data:
            cell = hashlib.sha256((row + previous).encode("utf-8")).hexdigest()
            previous = cell
            expected_result += cell
        expected_result = hashlib.sha256(expected_result.encode("utf-8")).hexdigest()
        self.assertEqual(result, expected_result, "Ошибка в calculate_row_hash")

    @patch('main.upload_to_ipfs', return_value="some_ipns_hash")
    def test_calculate_table_hash(self, val):
        table_data = [["some_data", "some_previous_data"], ["some_data", "some_previous_data"]]
        result = calculate_table_hash(table_data)
        expected_result = val.return_value
        self.assertEqual(result, expected_result, "Ошибка в calculate_table_hash")

