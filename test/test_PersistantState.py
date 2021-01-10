import os
from unittest import TestCase

from hexlib.db import PersistentState


class TestPersistentState(TestCase):

    def tearDown(self) -> None:
        os.remove("state.db")

    def setUp(self) -> None:
        os.remove("state.db")

    def test_get_set(self):
        s = PersistentState()

        val = {"a": 1, "b": "2", "c": b'3', "d": 4.4}
        s["a"]["1"] = val

        val["id"] = "1"

        self.assertDictEqual(val, s["a"]["1"])

    def test_get_set_int_id(self):
        s = PersistentState()

        val = {"a": 1, "b": "2", "c": b'3', "d": 4.4}
        s["a"][1] = val

        val["id"] = 1

        self.assertDictEqual(val, s["a"][1])
