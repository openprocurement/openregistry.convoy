# -*- coding: utf-8 -*-
import unittest
import json
import mock
from uuid import uuid4
from openregistry.convoy.utils import (
    push_filter_doc,
    continuous_changes_feed,
    FILTER_DOC_ID,
    FILTER_CONVOY_FEED_DOC
)


class AlmostAlwaysTrue(object):

    def __init__(self, total_iterations=1):
        self.total_iterations = total_iterations
        self.current_iteration = 0

    def __nonzero__(self):
        if self.current_iteration < self.total_iterations:
            self.current_iteration += 1
            return bool(1)
        return bool(0)


class TestUtilsSuite(unittest.TestCase):
    """ """

    def test_push_filter_doc(self):
        db = mock.MagicMock()
        db.get.side_effect = [None, {'_id': FILTER_DOC_ID, 'filters': {}}]
        filter_doc = {
            '_id': FILTER_DOC_ID,
            'filters': {
                'convoy_feed': FILTER_CONVOY_FEED_DOC
            }
        }
        push_filter_doc(db)
        db.get.assert_called_once_with(
            FILTER_DOC_ID, {'_id': FILTER_DOC_ID, 'filters': {}})
        self.assertEqual(db.save.call_count, 0)

        push_filter_doc(db)
        self.assertEqual(db.get.call_count, 2)
        db.save.assert_called_once_with(filter_doc)

    def test_continuous_changes_feed(self):
        db = mock.MagicMock()
        auction_id = uuid4().hex
        lot_id = uuid4().hex
        db.changes.side_effect = [
            {'last_seq': 1, 'results': [
                {'doc': {
                         '_id': auction_id,
                         'status': 'pending.verifcation',
                         'merchandisingObject': lot_id}}
            ]},
            {'last_seq': 2, 'results': []}
        ]
        with mock.patch(
                'openregistry.convoy.utils.CONTINUOUS_CHANGES_FEED_FLAG',
                AlmostAlwaysTrue(2)):
            results = []
            for r in continuous_changes_feed(db, timeout=0.1):
                results.append(r)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], {'merchandisingObject': lot_id,
                                      'id': auction_id,
                                      'status': 'pending.verifcation'
                                      })


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestUtilsSuite))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
