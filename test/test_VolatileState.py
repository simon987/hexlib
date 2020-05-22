from unittest import TestCase
from hexlib.db import VolatileState


class TestVolatileState(TestCase):

    def test_get_set(self):
        s = VolatileState(prefix="test1")
        val = {
            "field1": 1,
            "arr1": [1,2,3]
        }

        s["a"]["1"] = val

        self.assertDictEqual(val, s["a"]["1"])

    def test_iter(self):
        s = VolatileState(prefix="test2")

        s["b"]["1"] = 1
        s["b"]["2"] = 2
        s["b"]["3"] = 3
        s["b"]["4"] = 4

        self.assertEqual(sum(s["b"]), 10)

    def test_int_key(self):
        s = VolatileState(prefix="test2")
        s["b"][1] = 1
        del s["b"][1]

    def test_delete(self):
        s = VolatileState(prefix="test3")

        s["c"]["1"] = 1
        self.assertIsNotNone(s["c"]["1"])
        del s["c"]["1"]
        self.assertIsNone(s["c"]["1"])

