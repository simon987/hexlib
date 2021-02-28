from unittest import TestCase

from hexlib.misc import buffered


class TestBuffered(TestCase):

    def test_simple(self):
        my_list = []

        @buffered(batch_size=2)
        def put_item(items):
            my_list.extend(items)

        put_item([1, 2])
        put_item([1])
        put_item([1])
        put_item([1])

        self.assertEqual(len(my_list), 4)

    def test_flush(self):
        my_list = []

        @buffered(batch_size=2)
        def put_item(items):
            my_list.extend(items)

        put_item([1, 2])
        put_item([1])
        put_item([1])
        put_item([1])

        put_item(None)

        self.assertEqual(len(my_list), 5)
