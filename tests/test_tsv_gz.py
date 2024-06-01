import unittest
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from SimpleDataReader import SimpleDataReader


class TestSimpleDataReader(unittest.TestCase):
    def test_read_data_csv(self) -> None:
        sdr = SimpleDataReader()
        data = sdr.read_data_csv("test", "tests/data", header=True)
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0], ["1", "2", "3"])
        self.assertEqual(data[1], ["4", "5", "6"])
        self.assertEqual(data[2], ["7", "8", "9"])

    def test_read_data_json(self) -> None:
        sdr = SimpleDataReader()
        data = sdr.read_data_json("test", "tests/data")
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0], {"a": 1, "b": 2, "c": 3})
        self.assertEqual(data[1], {"a": 4, "b": 5, "c": 6})
        self.assertEqual(data[2], {"a": 7, "b": 8, "c": 9})


if __name__ == "__main__":
    unittest.main()
