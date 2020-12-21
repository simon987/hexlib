from unittest import TestCase
from hexlib.db import VolatileState, VolatileBooleanState


class TestVolatileState(TestCase):

    def test_get_set(self):
        s = VolatileState(prefix="test1")
        val = {
            "field1": 1,
            "arr1": [1, 2, 3]
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


class TestVolatileBoolState(TestCase):

    def test_get_set(self):
        s = VolatileBooleanState(prefix="test1")

        s["a"]["1"] = True
        s["a"]["2"] = True

        self.assertTrue(s["a"]["1"])
        self.assertTrue(s["a"]["2"])
        self.assertFalse(s["a"]["3"])

    def test_iter(self):
        s = VolatileBooleanState(prefix="test2")

        s["b"]["1"] = True
        s["b"]["2"] = True
        s["b"]["3"] = True
        s["b"]["4"] = True

        self.assertEqual(sum(int(x) for x in s["b"]), 10)

    def test_delete(self):
        s = VolatileBooleanState(prefix="test3")

        s["c"]["1"] = True
        self.assertTrue(s["c"]["1"])
        del s["c"]["1"]
        self.assertFalse(s["c"]["1"])
