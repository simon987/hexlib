import os
from datetime import datetime
from enum import Enum
from typing import Optional
from unittest import TestCase

from pydantic import BaseModel
from pydantic.types import List

from hexlib.db import PersistentState


class Status(Enum):
    yes = "yes"
    no = "no"


class Point(BaseModel):
    x: int
    y: int


class Polygon(BaseModel):
    points: List[Point] = []
    created_date: datetime
    status: Status = Status("yes")


class TestPydanticTable(TestCase):
    def tearDown(self) -> None:
        if os.path.exists("state.db"):
            os.remove("state.db")

    def setUp(self) -> None:
        if os.path.exists("state.db"):
            os.remove("state.db")

    def test_get_set(self):
        s = PersistentState()

        val = Polygon(
            created_date=datetime(year=2000, day=1, month=1),
            points=[
                Point(x=1, y=2),
                Point(x=3, y=4),
            ],
        )

        s["a"]["1"] = val

        self.assertEqual(s["a"]["1"].points[0].x, 1)
        self.assertEqual(s["a"]["1"].status, Status("yes"))
        self.assertEqual(s["a"]["1"].points[1].x, 3)
        self.assertEqual(s["a"]["1"].created_date.year, 2000)

    def test_update(self):
        s = PersistentState()

        val = Polygon(
            created_date=datetime(year=2000, day=1, month=1),
            points=[
                Point(x=1, y=2),
                Point(x=3, y=4),
            ]
        )

        s["a"]["1"] = val

        self.assertEqual(s["a"]["1"].points[0].x, 1)

        val.points[0].x = 2
        s["a"]["1"] = val
        self.assertEqual(s["a"]["1"].points[0].x, 2)

    def test_sql(self):
        s = PersistentState()

        s["b"]["1"] = Polygon(
            created_date=datetime(year=2000, day=1, month=1),
            points=[]
        )
        s["b"]["2"] = Polygon(
            created_date=datetime(year=2010, day=1, month=1),
            points=[]
        )

        result = list(s["b"].sql(
            "WHERE json->>'created_date' LIKE '2000-%'"
        ))

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].created_date.year, 2000)

    def test_iterate(self):
        s = PersistentState()

        s["b"]["1"] = Polygon(
            created_date=datetime(year=2000, day=1, month=1),
            points=[]
        )
        s["b"]["2"] = Polygon(
            created_date=datetime(year=2010, day=1, month=1),
            points=[]
        )

        result = list(s["b"])

        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].created_date.year, 2000)
        self.assertEqual(result[1].created_date.year, 2010)
