import os
from unittest import TestCase

from hexlib.db import PersistentState


class TestPersistentState(TestCase):

    def tearDown(self) -> None:
        if os.path.exists("state.db"):
            os.remove("state.db")

    def setUp(self) -> None:
        if os.path.exists("state.db"):
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

    def test_update_partial(self):
        s = PersistentState()

        val = {"a": 1, "b": "2", "c": b'3', "d": 4.4}
        s["a"][1] = val
        s["a"][1] = {
            "a": 2
        }

        val["a"] = 2
        val["id"] = 1

        self.assertDictEqual(val, s["a"][1])

    def test_none(self):
        s = PersistentState()

        val = {"a": 1, "b": None}
        s["a"][1] = val
        s["a"][1] = {
            "a": None
        }

        val["a"] = None
        val["id"] = 1

        self.assertDictEqual(val, s["a"][1])

    def test_bool(self):
        s = PersistentState()

        val = {"a": True, "b": False}
        s["a"][1] = val
        s["a"][1] = {
            "a": True
        }

        val["a"] = True
        val["id"] = 1

        self.assertDictEqual(val, s["a"][1])

    def test_sql(self):
        s = PersistentState()

        s["a"][1] = {"a": True}
        s["a"][2] = {"a": False}
        s["a"][3] = {"a": True}

        items = list(s["a"].sql("WHERE a=0 ORDER BY id"))

        self.assertDictEqual(items[0], s["a"][2])

    def test_delitem(self):
        s = PersistentState()

        s["a"][1] = {"a": True}
        del s["a"][1]

        self.assertIsNone(s["a"][1])

    def test_delitem_nonexistent(self):
        s = PersistentState()

        s["a"][1] = {"a": True}
        del s["a"][456]

        self.assertIsNotNone(s["a"][1])

    def test_delitem_no_table(self):
        s = PersistentState()

        try:
            del s["a"][456]
        except Exception as e:
            self.fail(e)

    def test_deserialize_get_set(self):
        s = PersistentState()

        s["a"][0] = {"x": b'abc'}

        self.assertEqual(s["a"][0]["x"], b'abc')

    def test_deserialize_sql(self):
        s = PersistentState()

        s["a"][0] = {"x": b'abc'}

        self.assertEqual(list(s["a"].sql("WHERE 1=1"))[0]["x"], b'abc')

    def test_deserialize_iter(self):
        s = PersistentState()

        s["a"][0] = {"x": b'abc'}

        self.assertEqual(list(s["a"])[0]["x"], b'abc')

    def test_drop_table(self):
        s = PersistentState()

        s["a"][0] = {"x": 1}
        s["a"][1] = {"x": 2}
        self.assertEqual(len(list(s["a"])), 2)

        del s["a"]
        self.assertEqual(len(list(s["a"])), 0)
